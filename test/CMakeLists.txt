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
  test_perf.cc
  test_segment.cc
  test_splitter.cc
)

# Ten translation units including one header: a definition that lost its
# `inline` fails to link here, which is the check that keeps the single header
# honest.
target_link_libraries(test-main PRIVATE gtest_main searchlib-segment)

include(GoogleTest)
gtest_discover_tests(test-main)
