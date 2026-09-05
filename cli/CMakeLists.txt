cmake_minimum_required(VERSION 3.14)
project(searchlib-cli)

add_executable(
  searchlib-cli
  main.cpp
  ../src/utils.cpp
  ../src/invertedindex.cpp
  ../src/compressedindex.cpp
  ../src/search.cpp
  ../src/query.cpp
  ../src/tokenizer.cpp
  ../src/analyzer.cpp
)

target_include_directories(searchlib-cli PRIVATE ../include ../src ../third_party)
