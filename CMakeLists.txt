# version 3.11 or later of CMake needed later for installing GoogleTest
# so let's require it now.
cmake_minimum_required(VERSION 3.11-3.18)

project(
    Yukon
    VERSION 0.1
    DESCRIPTION "Second longest river in canada. A stream processing sample with morden C++")

set( CMAKE_CXX_STANDARD 17)

include_directories(src/)
include_directories(/usr/local/include)
add_library(hello src/hello.cpp)

find_package(rxcpp CONFIG)

add_executable(main src/main.cpp)
target_link_libraries(main hello)