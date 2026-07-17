cmake_minimum_required(VERSION 3.14)
project(test)

include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/refs/tags/v1.17.0.zip
)
# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

enable_testing()

add_executable(
  test-main
  test.cc
  test_kjv.cc
  test_kjv_chapters.cc
  test_federated.cc
  test_analyzer.cc
  test_succinct.cc
  test_multifieldindex.cc
  ../src/utils.cpp
  ../src/invertedindex.cpp
  ../src/compressedindex.cpp
  ../src/search.cpp
  ../src/query.cpp
  ../src/tokenizer.cpp
  ../src/analyzer.cpp
  ../src/federatedindex.cpp
)

target_include_directories(test-main PRIVATE ../include ../src)
target_link_libraries(test-main PRIVATE gtest_main)

include(GoogleTest)
gtest_discover_tests(test-main)
