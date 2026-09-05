cmake_minimum_required(VERSION 3.14)
project(searchlib-cli)

add_executable(searchlib-cli main.cpp)

target_link_libraries(searchlib-cli PRIVATE searchlib)
