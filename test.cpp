#include <iostream>
#include <vector>
#include <string>
#include <chrono>
#include <cassert>
#include <fstream>
#include <sstream>
#include <random>
#include <algorithm>

#include "RTree.h"
#include "Type.h"
#include "Node.h"

using namespace SpatialStorage;

// 测试配置
struct TestConfig {
    int dimensions = 2;      // 维度
    int key_size = 4 * sizeof(double);  // 4个double: x1, y1, x2, y2
    int value_size = sizeof(uint64_t);  // 存储uint64_t值
    int block_size = 4096;   // 块大小
    int test_count = 1000;   // 测试数量
    std::string data_file;   // 数据文件
    bool use_file = false;   // 是否使用文件输入
};

// 操作类型
enum class Operation {
    INSERT,
    DELETE,
    OVERLAP_SEARCH,
    COMPRISE_SEARCH
};

// 测试数据条目
struct TestData {
    Operation op;
    RKeyType<double> key;
    uint64_t value;
    
    TestData(Operation o, const std::vector<double>& k, uint64_t v = 0)
        : op(o), key(k), value(v) {}
};

// 暴力搜索实现（用于对拍）
class BruteForceSearch {
private:
    std::vector<std::pair<RKeyType<double>, uint64_t>> data;
    
public:
    void insert(const RKeyType<double>& key, uint64_t value) {
        // 检查是否已存在
        for (auto& item : data) {
            if (item.first == key) {
                item.second = value; // 更新值
                return;
            }
        }
        data.emplace_back(key, value);
    }
    
    bool remove(const RKeyType<double>& key) {
        for (auto it = data.begin(); it != data.end(); ++it) {
            if (it->first == key) {
                data.erase(it);
                return true;
            }
        }
        return false;
    }
    
    std::vector<std::pair<RKeyType<double>, uint64_t>> overlap_search(const RKeyType<double>& query) {
        std::vector<std::pair<RKeyType<double>, uint64_t>> result;
        for (const auto& item : data) {
            if (item.first.IsOverlap(query)) {
                result.push_back(item);
            }
        }
        return result;
    }
    
    std::vector<std::pair<RKeyType<double>, uint64_t>> comprise_search(const RKeyType<double>& query) {
        std::vector<std::pair<RKeyType<double>, uint64_t>> result;
        for (const auto& item : data) {
            if (query >= item.first) { // query包含item.key
                result.push_back(item);
            }
        }
        return result;
    }
    
    size_t size() const { return data.size(); }
};

// 随机生成测试数据
std::vector<TestData> generate_test_data(int count, int dimensions) {
    std::vector<TestData> test_data;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<double> coord_dist(0.0, 100.0);
    std::uniform_int_distribution<int> op_dist(0, 3);
    std::uniform_int_distribution<uint64_t> value_dist(1, 10000);
    
    for (int i = 0; i < count; ++i) {
        Operation op = static_cast<Operation>(op_dist(gen));
        std::vector<double> key_data;
        
        // 生成矩形坐标 (x1, y1, x2, y2) 等
        for (int j = 0; j < dimensions * 2; ++j) {
            double coord = coord_dist(gen);
            key_data.push_back(coord);
        }
        
        // 确保x2 >= x1, y2 >= y1 等
        for (int d = 0; d < dimensions; ++d) {
            if (key_data[d] > key_data[d + dimensions]) {
                std::swap(key_data[d], key_data[d + dimensions]);
            }
        }
        
        uint64_t value = (op == Operation::INSERT) ? value_dist(gen) : 0;
        test_data.emplace_back(op, key_data, value);
    }
    
    return test_data;
}

// 从文件读取测试数据
std::vector<TestData> read_test_data_from_file(const std::string& filename, int dimensions) {
    std::vector<TestData> test_data;
    std::ifstream file(filename);
    
    if (!file.is_open()) {
        std::cerr << "无法打开文件: " << filename << std::endl;
        return test_data;
    }
    
    std::string line;
    while (std::getline(file, line)) {
        std::istringstream iss(line);
        std::string op_str;
        iss >> op_str;
        
        Operation op;
        if (op_str == "INSERT") op = Operation::INSERT;
        else if (op_str == "DELETE") op = Operation::DELETE;
        else if (op_str == "OVERLAP_SEARCH") op = Operation::OVERLAP_SEARCH;
        else if (op_str == "COMPRISE_SEARCH") op = Operation::COMPRISE_SEARCH;
        else continue;
        
        std::vector<double> key_data;
        double coord;
        for (int i = 0; i < dimensions * 2; ++i) {
            if (iss >> coord) {
                key_data.push_back(coord);
            }
        }
        
        uint64_t value = 0;
        if (op == Operation::INSERT) {
            iss >> value;
        }
        
        if (key_data.size() == dimensions * 2) {
            test_data.emplace_back(op, key_data, value);
        }
    }
    
    return test_data;
}

// 比较两个结果集是否相等
bool compare_results(
    const std::vector<std::pair<RKeyType<double>, uint64_t>>& brute_force_result,
    const std::vector<KeyValuePair<RKeyType<double>>*>& rtree_result) 
{
    if (brute_force_result.size() != rtree_result.size()) {
        return false;
    }
    
    // 由于顺序可能不同，需要排序后比较
    auto bf_sorted = brute_force_result;
    auto rt_sorted = rtree_result;
    
    // 按key排序 - 使用具体类型而不是auto
    std::sort(bf_sorted.begin(), bf_sorted.end(), 
              [](const std::pair<RKeyType<double>, uint64_t>& a, 
                 const std::pair<RKeyType<double>, uint64_t>& b) { 
                  return a.first.data < b.first.data; 
              });
    std::sort(rt_sorted.begin(), rt_sorted.end(), 
              [](const KeyValuePair<RKeyType<double>>* a, 
                 const KeyValuePair<RKeyType<double>>* b) { 
                  return a->key.data < b->key.data; 
              });
    
    for (size_t i = 0; i < bf_sorted.size(); ++i) {
        if (bf_sorted[i].first != rt_sorted[i]->key || 
            bf_sorted[i].second != *reinterpret_cast<const uint64_t*>(rt_sorted[i]->value)) {
            return false;
        }
    }
    
    return true;
}

// 执行测试
void run_test(const TestConfig& config) {
    std::cout << "=== R树测试开始 ===" << std::endl;
    std::cout << "维度: " << config.dimensions << std::endl;
    std::cout << "测试数量: " << config.test_count << std::endl;
    std::cout << "块大小: " << config.block_size << std::endl;
    
    // 生成或读取测试数据
    std::vector<TestData> test_data;
    if (config.use_file && !config.data_file.empty()) {
        std::cout << "从文件读取测试数据: " << config.data_file << std::endl;
        test_data = read_test_data_from_file(config.data_file, config.dimensions);
        if (test_data.empty()) {
            std::cout << "文件为空或读取失败，使用随机数据" << std::endl;
            test_data = generate_test_data(config.test_count, config.dimensions);
        }
    } else {
        std::cout << "生成随机测试数据" << std::endl;
        test_data = generate_test_data(config.test_count, config.dimensions);
    }
    
    std::cout<<"初始化对拍..." << std::endl;
    // 创建R树和暴力搜索
    auto rtree = RTree<double>::create(AT_FDCWD, "test_rtree.index", 
                                      config.key_size, config.value_size,
                                      config.block_size, config.dimensions);
    
    BruteForceSearch brute_force;
    std::cout<<"对拍初始化完成" << std::endl;
    
    // 统计信息
    int success_count = 0;
    int total_operations = test_data.size();
    double total_rtree_time = 0.0;
    double total_brute_force_time = 0.0;
    
    // 执行测试
    for (size_t i = 0; i < test_data.size(); ++i) {
        const auto& data = test_data[i];
        bool success = true;
        
        std::cout << "\n操作 " << (i + 1) << "/" << test_data.size() << ": ";
        
        switch (data.op) {
            case Operation::INSERT: {
                std::cout << "INSERT ";
                for (double coord : data.key.data) {
                    std::cout << coord << " ";
                }
                std::cout << "value=" << data.value;
                
                // R树插入
                auto start = std::chrono::high_resolution_clock::now();
                KeyValuePair<RKeyType<double>> kvp{data.key, reinterpret_cast<const void*>(&data.value)};
                rtree.Insert(kvp);
                auto end = std::chrono::high_resolution_clock::now();
                double rtree_time = std::chrono::duration<double, std::milli>(end - start).count();
                
                // 暴力搜索插入
                start = std::chrono::high_resolution_clock::now();
                brute_force.insert(data.key, data.value);
                end = std::chrono::high_resolution_clock::now();
                double bf_time = std::chrono::duration<double, std::milli>(end - start).count();
                
                total_rtree_time += rtree_time;
                total_brute_force_time += bf_time;
                
                std::cout << " - R树: " << rtree_time << "ms, 暴力: " << bf_time << "ms";
                break;
            }
            
            case Operation::DELETE: {
                std::cout << "DELETE ";
                for (double coord : data.key.data) {
                    std::cout << coord << " ";
                }
                
                // R树删除
                auto start = std::chrono::high_resolution_clock::now();
                KeyValuePair<RKeyType<double>> kvp{data.key, nullptr};
                bool rtree_result = rtree.Delete(kvp);
                auto end = std::chrono::high_resolution_clock::now();
                double rtree_time = std::chrono::duration<double, std::milli>(end - start).count();
                
                // 暴力搜索删除
                start = std::chrono::high_resolution_clock::now();
                bool bf_result = brute_force.remove(data.key);
                end = std::chrono::high_resolution_clock::now();
                double bf_time = std::chrono::duration<double, std::milli>(end - start).count();
                
                total_rtree_time += rtree_time;
                total_brute_force_time += bf_time;
                
                // 检查结果一致性
                if (rtree_result != bf_result) {
                    std::cout << " - 错误: 删除结果不一致 (R树: " << rtree_result 
                              << ", 暴力: " << bf_result << ")";
                    success = false;
                } else {
                    std::cout << " - R树: " << rtree_time << "ms, 暴力: " << bf_time << "ms";
                }
                break;
            }
            
            case Operation::OVERLAP_SEARCH: {
                std::cout << "OVERLAP_SEARCH ";
                for (double coord : data.key.data) {
                    std::cout << coord << " ";
                }
                
                // R树搜索
                auto start = std::chrono::high_resolution_clock::now();
                auto rtree_result = rtree.Overlap_Search(data.key);
                auto end = std::chrono::high_resolution_clock::now();
                double rtree_time = std::chrono::duration<double, std::milli>(end - start).count();
                
                // 暴力搜索
                start = std::chrono::high_resolution_clock::now();
                auto bf_result = brute_force.overlap_search(data.key);
                end = std::chrono::high_resolution_clock::now();
                double bf_time = std::chrono::duration<double, std::milli>(end - start).count();
                
                total_rtree_time += rtree_time;
                total_brute_force_time += bf_time;
                
                // 检查结果一致性
                if (!compare_results(bf_result, rtree_result)) {  // 移除解引用
                    std::cout << " - 错误: 搜索结果不一致 (R树找到 " << rtree_result.size()  // 使用 .size()
                              << " 个, 暴力找到 " << bf_result.size() << " 个)";
                    success = false;
                } else {
                    std::cout << " - 找到 " << bf_result.size() << " 个结果, R树: " 
                              << rtree_time << "ms, 暴力: " << bf_time << "ms";
                }
                break;
            }
            
            case Operation::COMPRISE_SEARCH: {
                std::cout << "COMPRISE_SEARCH ";
                for (double coord : data.key.data) {
                    std::cout << coord << " ";
                }
                
                // R树搜索
                auto start = std::chrono::high_resolution_clock::now();
                auto rtree_result = rtree.Comprise_Search(data.key);
                auto end = std::chrono::high_resolution_clock::now();
                double rtree_time = std::chrono::duration<double, std::milli>(end - start).count();
                
                // 暴力搜索
                start = std::chrono::high_resolution_clock::now();
                auto bf_result = brute_force.comprise_search(data.key);
                end = std::chrono::high_resolution_clock::now();
                double bf_time = std::chrono::duration<double, std::milli>(end - start).count();
                
                total_rtree_time += rtree_time;
                total_brute_force_time += bf_time;
                
                // 检查结果一致性
                if (!compare_results(bf_result, rtree_result)) {  // 移除解引用
                    std::cout << " - 错误: 搜索结果不一致 (R树找到 " << rtree_result.size()  // 使用 .size()
                              << " 个, 暴力找到 " << bf_result.size() << " 个)";
                    success = false;
                } else {
                    std::cout << " - 找到 " << bf_result.size() << " 个结果, R树: " 
                              << rtree_time << "ms, 暴力: " << bf_time << "ms";
                }
                break;
            }
        }
        
        if (success) {
            success_count++;
        } else {
            std::cout << " [失败]";
        }
    }
    
    // 输出统计信息
    std::cout << "\n\n=== 测试结果 ===" << std::endl;
    std::cout << "总操作数: " << total_operations << std::endl;
    std::cout << "成功操作: " << success_count << std::endl;
    std::cout << "成功率: " << (success_count * 100.0 / total_operations) << "%" << std::endl;
    std::cout << "R树总时间: " << total_rtree_time << "ms" << std::endl;
    std::cout << "暴力搜索总时间: " << total_brute_force_time << "ms" << std::endl;
    if (total_rtree_time > 0) {
        std::cout << "加速比: " << (total_brute_force_time / total_rtree_time) << "x" << std::endl;
    }
    std::cout << "最终数据量: " << brute_force.size() << " 个条目" << std::endl;
}

// 交互式测试
void interactive_test() {
    TestConfig config;
    
    std::cout << "=== R树交互式测试 ===" << std::endl;
    std::cout << "选择输入方式:" << std::endl;
    std::cout << "1. 随机生成测试数据" << std::endl;
    std::cout << "2. 从文件读取测试数据" << std::endl;
    
    int choice;
    std::cin >> choice;
    
    if (choice == 2) {
        config.use_file = true;
        std::cout << "输入数据文件路径: ";
        std::cin >> config.data_file;
    } else {
        std::cout << "输入测试数据数量: ";
        std::cin >> config.test_count;
    }
    
    std::cout << "输入维度数: ";
    std::cin >> config.dimensions;
    
    run_test(config);
}

int main(int argc, char* argv[]) {
    if (argc > 1) {
        // 命令行模式
        TestConfig config;
        
        for (int i = 1; i < argc; ++i) {
            std::string arg = argv[i];
            if (arg == "-f" && i + 1 < argc) {
                config.use_file = true;
                config.data_file = argv[++i];
            } else if (arg == "-n" && i + 1 < argc) {
                config.test_count = std::stoi(argv[++i]);
            } else if (arg == "-d" && i + 1 < argc) {
                config.dimensions = std::stoi(argv[++i]);
            } else if (arg == "-b" && i + 1 < argc) {
                config.block_size = std::stoi(argv[++i]);
            }
        }
        
        run_test(config);
    } else {
        // 交互式模式
        interactive_test();
    }
    
    return 0;
}