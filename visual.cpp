#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <memory>
#include <SFML/Graphics.hpp>
#include <SFML/Window.hpp>

#include "RTree.h"
#include "Type.h"

using namespace SpatialStorage;

// 操作类型
enum class OperationType {
    INSERT,
    DELETE,
    OVERLAP_SEARCH,
    COMPRISE_SEARCH
};

// 操作命令
struct Operation {
    OperationType type;
    std::vector<double> key_data;  // MBR坐标 [x1, y1, x2, y2, ...]
    uint64_t value;
    
    Operation(OperationType t, const std::vector<double>& data, uint64_t v = 0)
        : type(t), key_data(data), value(v) {}
};

// 读取操作文件
std::vector<Operation> read_operations(const std::string& filename) {
    std::vector<Operation> operations;
    std::ifstream file(filename);
    
    if (!file.is_open()) {
        std::cerr << "无法打开操作文件: " << filename << std::endl;
        return operations;
    }
    
    std::string line;
    while (std::getline(file, line)) {
        std::istringstream iss(line);
        std::string op_str;
        iss >> op_str;
        
        OperationType op_type;
        if (op_str == "INSERT") op_type = OperationType::INSERT;
        else if (op_str == "DELETE") op_type = OperationType::DELETE;
        else if (op_str == "OVERLAP_SEARCH") op_type = OperationType::OVERLAP_SEARCH;
        else if (op_str == "COMPRISE_SEARCH") op_type = OperationType::COMPRISE_SEARCH;
        else continue;
        
        std::vector<double> key_data;
        double coord;
        while (iss >> coord) {
            key_data.push_back(coord);
        }
        
        uint64_t value = 0;
        if (op_type == OperationType::INSERT && key_data.size() >= 4) {
            // 最后一个数字作为value
            value = static_cast<uint64_t>(key_data.back());
            key_data.pop_back();
        }
        
        if (key_data.size() >= 4) {  // 至少需要4个坐标（2D MBR）
            operations.emplace_back(op_type, key_data, value);
        }
    }
    
    return operations;
}

// 可视化应用类
class RTreeVisualizer {
private:
    sf::RenderWindow window;
    std::vector<Operation> operations;
    size_t current_op_index;
    RTree<double> rtree;
    std::vector<KeyValuePair<KeyType<double>>> current_search_results;
    KeyType<double> current_search_range;
    bool has_search;
    
    // 颜色定义
    const sf::Color BG_COLOR = sf::Color::White;
    const sf::Color MBR_COLOR = sf::Color::Blue;
    const sf::Color SEARCH_RESULT_COLOR = sf::Color::Red;
    const sf::Color SEARCH_RANGE_COLOR = sf::Color(255, 165, 0, 128); // 半透明橙色
    const sf::Color TEXT_COLOR = sf::Color::Black;
    
    // 坐标变换参数
    const float PADDING = 50.0f;
    const float WORLD_SIZE = 100.0f;  // 假设坐标范围是[0, 100]
    
public:
    RTreeVisualizer(const std::string& op_file)
        : window(sf::VideoMode(1200, 800), "R-Tree Visual Demo"),
          operations(read_operations(op_file)),
          current_op_index(0),
          rtree(RTree<double>::create(AT_FDCWD, "visual_demo.index", 
                                    4 * sizeof(double), sizeof(uint64_t), 
                                    4096, 2)),
          has_search(false)
    {
        window.setFramerateLimit(60);
    }
    
    void run() {
        std::cout << "R-Tree 可视化Demo" << std::endl;
        std::cout << "按回车键执行下一条操作" << std::endl;
        std::cout << "按ESC键退出" << std::endl;
        
        while (window.isOpen()) {
            handleEvents();
            render();
        }
        
        // 清理临时文件
        unlink("visual_demo.index");
    }
    
private:
    void handleEvents() {
        sf::Event event;
        while (window.pollEvent(event)) {
            if (event.type == sf::Event::Closed) {
                window.close();
            }
            else if (event.type == sf::Event::KeyPressed) {
                if (event.key.code == sf::Keyboard::Escape) {
                    window.close();
                }
                else if (event.key.code == sf::Keyboard::Enter) {
                    executeNextOperation();
                }
            }
        }
    }
    
    void executeNextOperation() {
        if (current_op_index >= operations.size()) {
            std::cout << "所有操作已执行完毕!" << std::endl;
            return;
        }
        
        const auto& op = operations[current_op_index];
        has_search = false;
        
        std::cout << "执行操作 " << (current_op_index + 1) << "/" << operations.size() << ": ";
        
        switch (op.type) {
            case OperationType::INSERT: {
                std::cout << "INSERT - MBR[";
                for (size_t i = 0; i < op.key_data.size(); ++i) {
                    std::cout << op.key_data[i];
                    if (i < op.key_data.size() - 1) std::cout << ", ";
                }
                std::cout << "], value=" << op.value << std::endl;
                
                KeyValuePair<KeyType<double>> kvp{KeyType<double>(op.key_data), 
                                                 reinterpret_cast<const void*>(&op.value)};
                rtree.Insert(kvp);
                break;
            }
                
            case OperationType::DELETE: {
                std::cout << "DELETE - MBR[";
                for (size_t i = 0; i < op.key_data.size(); ++i) {
                    std::cout << op.key_data[i];
                    if (i < op.key_data.size() - 1) std::cout << ", ";
                }
                std::cout << "]" << std::endl;
                
                KeyValuePair<KeyType<double>> kvp{KeyType<double>(op.key_data), nullptr};
                rtree.Delete(kvp);
                break;
            }
                
            case OperationType::OVERLAP_SEARCH: {
                std::cout << "OVERLAP_SEARCH - Range[";
                for (size_t i = 0; i < op.key_data.size(); ++i) {
                    std::cout << op.key_data[i];
                    if (i < op.key_data.size() - 1) std::cout << ", ";
                }
                std::cout << "]" << std::endl;
                
                current_search_range = KeyType<double>(op.key_data);
                auto results = rtree.Overlap_Search(current_search_range);
                current_search_results.assign(results.begin(), results.end());
                has_search = true;
                break;
            }
                
            case OperationType::COMPRISE_SEARCH: {
                std::cout << "COMPRISE_SEARCH - Range[";
                for (size_t i = 0; i < op.key_data.size(); ++i) {
                    std::cout << op.key_data[i];
                    if (i < op.key_data.size() - 1) std::cout << ", ";
                }
                std::cout << "]" << std::endl;
                
                current_search_range = KeyType<double>(op.key_data);
                auto results = rtree.Comprise_Search(current_search_range);
                current_search_results.assign(results.begin(), results.end());
                has_search = true;
                break;
            }
        }
        
        current_op_index++;
    }
    
    void render() {
        window.clear(BG_COLOR);
        
        // 绘制搜索范围（如果有）
        if (has_search) {
            drawSearchRange();
        }
        
        // 绘制所有MBR
        drawAllMBRs();
        
        // 绘制搜索结果的MBR（如果有）
        if (has_search) {
            drawSearchResults();
        }
        
        // 绘制状态信息
        drawStatusInfo();
        
        window.display();
    }
    
    // 坐标变换：世界坐标 -> 屏幕坐标
    sf::Vector2f worldToScreen(float x, float y) {
        float scale_x = (window.getSize().x - 2 * PADDING) / WORLD_SIZE;
        float scale_y = (window.getSize().y - 2 * PADDING) / WORLD_SIZE;
        
        return sf::Vector2f(
            PADDING + x * scale_x,
            window.getSize().y - PADDING - y * scale_y  // 翻转Y轴
        );
    }
    
    void drawSearchRange() {
        const auto& data = current_search_range.getData();
        if (data.size() < 4) return;
        
        float x1 = data[0], y1 = data[1], x2 = data[2], y2 = data[3];
        auto top_left = worldToScreen(x1, y2);  // 注意Y轴翻转
        auto bottom_right = worldToScreen(x2, y1);
        
        sf::RectangleShape rect(bottom_right - top_left);
        rect.setPosition(top_left);
        rect.setFillColor(SEARCH_RANGE_COLOR);
        rect.setOutlineColor(sf::Color::Yellow);
        rect.setOutlineThickness(2);
        
        window.draw(rect);
    }
    
    void drawAllMBRs() {
        // 使用新的遍历接口获取所有MBR
        auto all_entries = rtree.GetAllEntries();
        
        for (const auto& entry : all_entries) {
            drawMBR(entry.key, MBR_COLOR, false);
            
            // 可选：在MBR中心显示value
            // drawValueText(entry);
        }
    }

    
    void drawSearchResults() {
        for (const auto& result : current_search_results) {
            drawMBR(result.key, SEARCH_RESULT_COLOR, true);
        }
    }
    
    void drawMBR(const KeyType<double>& mbr, const sf::Color& color, bool is_search_result) {
        const auto& data = mbr.getData();
        if (data.size() < 4) return;
        
        float x1 = data[0], y1 = data[1], x2 = data[2], y2 = data[3];
        auto top_left = worldToScreen(x1, y2);
        auto bottom_right = worldToScreen(x2, y1);
        
        sf::RectangleShape rect(bottom_right - top_left);
        rect.setPosition(top_left);
        rect.setFillColor(sf::Color::Transparent);
        rect.setOutlineColor(color);
        rect.setOutlineThickness(is_search_result ? 3 : 2);
        
        window.draw(rect);
        
        // 为搜索结果显示value
        if (is_search_result) {
            // 这里可以添加value文本显示
        }
    }
    
    void drawStatusInfo() {
        sf::Font font;
        // SFML需要字体文件，这里简单用控制台输出代替
        // 在实际使用中，你需要提供一个字体文件
        
        std::string status = "操作: " + std::to_string(current_op_index) + "/" + 
                           std::to_string(operations.size()) + 
                           " (按Enter继续, ESC退出)";
        
        // 简单的文本渲染（需要字体文件）
        // 这里先输出到控制台
        // std::cout << status << std::endl;
    }
};

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cout << "用法: " << argv[0] << " <操作文件>" << std::endl;
        std::cout << "操作文件格式示例:" << std::endl;
        std::cout << "INSERT 10 20 30 40 100" << std::endl;
        std::cout << "INSERT 50 60 70 80 200" << std::endl;
        std::cout << "OVERLAP_SEARCH 15 25 35 45" << std::endl;
        std::cout << "DELETE 10 20 30 40" << std::endl;
        return 1;
    }
    
    RTreeVisualizer visualizer(argv[1]);
    visualizer.run();
    
    return 0;
}