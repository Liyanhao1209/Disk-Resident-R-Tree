cmake_minimum_required(VERSION 3.15)
project(SpatialStorage CXX)

# 设置C++标准
set(CMAKE_CXX_STANDARD 17)  # 改为17，因为SFML需要C++17
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 设置编译选项
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_compile_options(-g -O0 -Wall -Wextra)
else()
    add_compile_options(-O2 -DNDEBUG)
endif()

# 查找SFML库
find_package(SFML 2.5 COMPONENTS graphics window system REQUIRED)

# 添加FileCache子目录
add_subdirectory(FileCache)

# 创建接口库（用于header-only库）
add_library(SpatialStorage INTERFACE)

# 使用生成器表达式正确设置包含目录
target_include_directories(SpatialStorage INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/FileCache/include>
    $<INSTALL_INTERFACE:include>
)

# 添加编译定义（如果需要）
target_compile_definitions(SpatialStorage INTERFACE 
    _FILE_OFFSET_BITS=64
)

# 测试程序
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/test.cpp")
    add_executable(SpatialStorageTest test.cpp)
    target_link_libraries(SpatialStorageTest SpatialStorage FileCache)
    set_target_properties(SpatialStorageTest PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
    )
    
    # 添加测试
    enable_testing()
    add_test(NAME SpatialStorageTest 
             COMMAND SpatialStorageTest
             WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
endif()

# 可视化Demo程序
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/visual.cpp")
    add_executable(VisualDemo visual.cpp)
    target_link_libraries(VisualDemo SpatialStorage FileCache sfml-graphics sfml-window sfml-system)
    set_target_properties(VisualDemo PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
    )
    
    # 在Linux上可能需要链接pthread
    if(UNIX AND NOT APPLE)
        target_link_libraries(VisualDemo pthread)
    endif()
endif()