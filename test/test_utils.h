#include <sstream>

#include "lib/unicodelib.h"
#include "utils.h"

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

