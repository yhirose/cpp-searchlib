cmake_minimum_required(VERSION 3.14)
project(searchlib-cli)

# The command-line parser, a header of its own so the tests can reach it
# without reaching into this directory.
add_library(searchlib-cli-args INTERFACE)
target_include_directories(searchlib-cli-args INTERFACE lib)

add_executable(searchlib-cli main.cpp)

target_link_libraries(searchlib-cli PRIVATE searchlib searchlib-cli-args)
searchlib_warnings(searchlib-cli)
