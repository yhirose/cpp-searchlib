#include <sstream>

#include "lib/unicodelib.h"
#include "utils.h"

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

