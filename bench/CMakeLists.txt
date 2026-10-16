cmake_minimum_required(VERSION 3.14)
project(searchlib-bench)

add_executable(searchlib-bench bench.cpp)

# searchlib-segment for --splitter; it carries searchlib itself.
target_link_libraries(searchlib-bench PRIVATE searchlib-segment)

searchlib_warnings(searchlib-bench)

# The corpus tool for the cross-engine comparison (see README.md); built here
# so that CI keeps it compiling.
add_executable(searchlib-segment-corpus segment_corpus.cpp)
target_link_libraries(searchlib-segment-corpus PRIVATE searchlib-segment)
searchlib_warnings(searchlib-segment-corpus)
