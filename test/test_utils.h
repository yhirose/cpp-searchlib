#include <algorithm>
#include <chrono>
#include <limits>
#include <sstream>

#include "unicodelib/unicodelib.h"
#include "utils.h"

// Minimum of several runs: the fastest observed run is the one least
// polluted by scheduling noise, so it is the stablest estimator here.
// Shared between the perf-regression tests and the benchmark harness so the
// two always report comparable numbers -- bench/bench.cpp's cross-run
// comparisons assume this exact estimator.
template <typename F> double best_of(size_t runs, F f) {
  f(); // warm up caches and any one-time allocation
  double best = std::numeric_limits<double>::max();
  for (size_t i = 0; i < runs; i++) {
    auto start = std::chrono::steady_clock::now();
    f();
    auto end = std::chrono::steady_clock::now();
    best = std::min(
        best, std::chrono::duration<double, std::micro>(end - start).count());
  }
  return best;
}

inline bool close_enough(double expect, double actual) {
  auto tolerance = 0.001;
  return (expect - tolerance) <= actual && actual <= (expect + tolerance);
}

#define EXPECT_AP(a, b)  EXPECT_TRUE(close_enough(a, b))

inline std::u32string to_lowercase(std::u32string str) {
  std::transform(str.begin(), str.end(), str.begin(),
                 [](auto c) { return std::tolower(c); });
  return str;
}

inline std::vector<std::string> split(const std::string &input,
                                      char delimiter) {
  std::istringstream ss(input);
  std::string field;
  std::vector<std::string> result;
  while (std::getline(ss, field, delimiter)) {
    result.push_back(field);
  }
  return result;
}

