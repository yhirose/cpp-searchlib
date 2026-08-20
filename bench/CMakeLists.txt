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
  ../src/federatedindex.cpp
)

target_include_directories(searchlib-bench PRIVATE ../include ../src ../third_party)
