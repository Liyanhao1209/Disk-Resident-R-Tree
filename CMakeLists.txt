cmake_minimum_required(VERSION 3.10)
project(RTree)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_subdirectory(FileCache)

include_directories(include)

file(GLOB R_TREE_SOURCES "src/*.cpp")

add_executable(RTree ${R_TREE_SOURCES})

target_link_libraries(RTree FileCache)