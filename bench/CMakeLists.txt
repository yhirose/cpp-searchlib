cmake_minimum_required(VERSION 3.14)
project(searchlib-bench)

add_executable(
  searchlib-bench
  bench.cpp
  ../src/utils.cpp
  ../src/invertedindex.cpp
  ../src/compressedindex.cpp
  ../src/search.cpp
  ../src/query.cpp
  ../src/tokenizer.cpp
  ../src/analyzer.cpp
)

target_include_directories(searchlib-bench PRIVATE ../include ../src ../third_party)

# For --splitter. Same arrangement as test/: searchlib-segment carries the only
# include path to the vendored cpp-segmentlib, and linking it does not extend
# that path to bench.cpp, which sees just include/searchlib_segment.h.
target_link_libraries(searchlib-bench PRIVATE searchlib-segment)
