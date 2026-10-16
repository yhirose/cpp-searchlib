//
//  bench_options.h
//
//  Copyright (c) 2026 Yuji Hirose. All rights reserved.
//  MIT License
//

// Command-line option values for the bench tools (bench.cpp,
// segment_corpus.cpp), shared so the two report a bad value the same way:
// one line naming the program and the option, then exit status 1.

#pragma once

#include <charconv>
#include <cstdio>
#include <cstdlib>
#include <string>

namespace bench_options {

// argv[i], the value of option `what`, or exit when the command line ends
// before it.
inline std::string need_value(int argc, char **argv, int i, const char *prog,
                              const char *what) {
  if (i >= argc) {
    std::fprintf(stderr, "%s: %s needs a value\n", prog, what);
    std::exit(1);
  }
  return argv[i];
}

// The value of a counting option, or exit. from_chars rather than std::stoul,
// which throws on "abc" without naming the option, reads "12x" as 12 and
// wraps "-1" round to the largest size_t. `min` is 1 where 0 would mean no
// run at all rather than a smaller one.
inline size_t parse_count(const std::string &value, const char *prog,
                          const char *what, size_t min = 0) {
  size_t n = 0;
  auto end = value.data() + value.size();
  auto [ptr, ec] = std::from_chars(value.data(), end, n);
  if (value.empty() || ec != std::errc() || ptr != end || n < min) {
    std::fprintf(stderr, "%s: %s takes an integer of at least %zu, got '%s'\n",
                 prog, what, min, value.c_str());
    std::exit(1);
  }
  return n;
}

} // namespace bench_options
