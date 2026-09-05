cmake_minimum_required(VERSION 3.14)
project(searchlib-bench)

add_executable(searchlib-bench bench.cpp)

# searchlib-segment for --splitter; it carries searchlib itself.
target_link_libraries(searchlib-bench PRIVATE searchlib-segment)
