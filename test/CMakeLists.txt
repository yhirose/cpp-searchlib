cmake_minimum_required(VERSION 3.14)
project(test)

add_executable(
  test-main test.cc
  ../src/utils.cpp
  ../src/indexing.cpp
  ../src/search.cpp
  ../src/query.cpp
  ../src/textrange.cpp
  ../src/tokenizer.cpp
)

target_include_directories(test-main PRIVATE ../include ../src)

enable_testing()

add_test(
  NAME TestMain
  COMMAND test-main
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
