//
//  analyzer.cpp
//
//  Copyright (c) 2021 Yuji Hirose. All rights reserved.
//  MIT License
//

#include "searchlib.h"

namespace searchlib {

TermFilter compose(std::vector<TermFilter> filters) {
  return [filters = std::move(filters)](
             const std::u32string &str,
             std::function<void(std::u32string)> emit) {
    // Feed each surviving token of stage i into stage i+1; reaching the end
    // of the chain emits it. Drops (a stage that never calls its emit) and
    // 1->N expansions fall out naturally from how often each stage emits.
    std::function<void(size_t, const std::u32string &)> apply =
        [&](size_t i, const std::u32string &s) {
          if (i == filters.size()) {
            emit(s);
            return;
          }
          filters[i](s, [&](std::u32string out) { apply(i + 1, out); });
        };
    apply(0, str);
  };
}

TermFilter to_term_filter(Normalizer normalizer) {
  return [normalizer = std::move(normalizer)](
             const std::u32string &str,
             std::function<void(std::u32string)> emit) {
    emit(normalizer ? normalizer(str) : str);
  };
}

} // namespace searchlib
