cmake_minimum_required(VERSION 3.14)
project(cpp-searchlib)

if(MSVC)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /Zc:__cplusplus /utf-8")
endif()

# The library itself: one header, plus the include paths its three vendored
# dependencies resolve on. searchlib.h spells them as <fstlib.h>, <peglib.h>,
# <unicodelib.h> and <unicodelib_encodings.h>, so a project that already
# vendors any of them points at its own copy instead of this one and gets a
# single definition of each (which is what culebra does).
add_library(searchlib INTERFACE)
target_compile_features(searchlib INTERFACE cxx_std_17)
target_include_directories(searchlib INTERFACE include)
target_include_directories(searchlib SYSTEM INTERFACE
  third_party/fstlib third_party/peglib third_party/unicodelib)

# The opt-in CJK segmentation splitter (include/searchlib_segment.h), which is
# a second header rather than part of searchlib.h because it needs segmentlib
# compiled in and a model file shipped. Its include root is `third_party`
# itself, since segmentlib's headers refer to each other as `segmentlib/...`,
# and SYSTEM because that tree is not warning-clean at -Wall -Wextra (nine
# -Wunused-parameter hits).
# segmenter.h uses std::thread, which older glibc keeps in libpthread.
find_package(Threads REQUIRED)
add_library(searchlib-segment INTERFACE)
target_link_libraries(searchlib-segment INTERFACE searchlib Threads::Threads)
target_include_directories(searchlib-segment SYSTEM INTERFACE third_party)

# Warnings for what is built here, not for whoever links the library: the
# header is meant to stay clean under -Wall -Wextra, and building the tests,
# the CLI and the benchmark with them is what keeps it so.
function(searchlib_warnings target)
  if(NOT MSVC)
    target_compile_options(${target} PRIVATE -Wall -Wextra)
  endif()
endfunction()

add_subdirectory(cli)
add_subdirectory(bench)
add_subdirectory(test)

enable_testing()

# install(FILES include/searchlib.h DESTINATION include)
