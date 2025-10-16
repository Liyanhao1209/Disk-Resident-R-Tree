# FileCache 模块的CMake配置

# 查找源文件
file(GLOB SOURCES "src/*.cpp")

# 如果没有源文件，检查是否在父目录的src中
if(NOT SOURCES)
    file(GLOB SOURCES "../src/FileCache.cpp")
endif()

if(SOURCES)
    # 如果有源文件，创建普通库
    add_library(FileCache ${SOURCES})
    
    # 包含目录 - 使用生成器表达式
    target_include_directories(FileCache PUBLIC 
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    )
    
    # 设置编译属性
    target_compile_definitions(FileCache PRIVATE 
        _FILE_OFFSET_BITS=64
    )
else()
    # 如果没有源文件，创建接口库
    add_library(FileCache INTERFACE)
    target_include_directories(FileCache INTERFACE 
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    )
endif()

# 设置输出目录
set_target_properties(FileCache PROPERTIES
    ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib
    LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib
)