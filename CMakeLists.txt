cmake_minimum_required(VERSION 3.14)
project(cpp-searchlib)

set(CMAKE_CXX_STANDARD 17)

if(MSVC)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /Zc:__cplusplus /utf-8")
endif()

# The CJK-segmentation splitter, built as its own target so that the include
# path reaching the vendored cpp-segmentlib (and, through it, segmentlib's own
# copy of fstlib) is scoped to this single translation unit. Including
# src/termdict.h alongside those headers fails with `namespace fst`
# redefinition errors, so the scoping is what makes the collision unreachable;
# see third_party/segmentlib/README.md. Declared at the top level rather than under
# test/ because it is a library of this project, not of its test suite; only
# test/ links it today, but nothing about it is test-specific.
#
# OBJECT rather than STATIC because consumers are executables built in this
# same tree; there is no install rule to carry a library artifact. It holds
# only this one file: the utils.cpp it calls into is compiled by whichever
# target links this, the way every other src/*.cpp here is.
add_library(searchlib-segment OBJECT src/segmentsplitter.cpp)
target_include_directories(searchlib-segment PRIVATE include src)
# -isystem, not -I: segmentlib's automaton is third-party code and is not
# warning-clean at -Wall -Wextra (nine -Wunused-parameter hits). This project
# sets no warning flags of its own today, so keeping it a system header is what
# makes a warnings-on build stay quiet whenever someone turns them on.
target_include_directories(searchlib-segment SYSTEM PRIVATE third_party)

add_subdirectory(cli)
add_subdirectory(bench)
add_subdirectory(test)

enable_testing()

# install(FILES searchlib.h DESTINATION include)
