# version 3.11 or later of CMake needed later for installing GoogleTest
# so let's require it now.
cmake_minimum_required(VERSION 3.11-3.18)

project(
    Yukon
    VERSION 0.1
    DESCRIPTION "Second longest river in canada. A stream processing sample with morden C++")

set( CMAKE_CXX_STANDARD 17)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src/)
include_directories(/usr/local/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/vendor/)

add_library(hello ${CMAKE_CURRENT_SOURCE_DIR}/src/hello.cpp)

find_package(rxcpp CONFIG)

add_executable(main ${CMAKE_CURRENT_SOURCE_DIR}/src/main.cpp)
target_link_libraries(main hello)
target_link_libraries(main ${CMAKE_CURRENT_SOURCE_DIR}/vendor/cxxfaker/cxxfaker.a)