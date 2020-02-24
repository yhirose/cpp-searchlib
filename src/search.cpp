//
//  search.cpp
//
//  Copyright (c) 2021 Yuji Hirose. All rights reserved.
//  MIT License
//

#include "searchlib.h"

namespace searchlib {

SearchResult::~SearchResult() = default;

class SearchResultForTermOperation : public SearchResult {
 public:
  SearchResultForTermOperation(const Index &index, size_t term_id)
      : index_(index), positional_list_(index.positional_index.at(term_id)) {}

  ~SearchResultForTermOperation() override = default;

  size_t document_count() const override { return positional_list_.size(); }

  size_t document_id(size_t index) const override {
    return positional_list_iter(index)->first;
  }

  size_t search_hit_count(size_t index) const override {
    return positional_list_iter(index)->second.size();
  }

  size_t term_position(size_t index, size_t search_hit_index) const override {
    return positional_list_iter(index)->second[search_hit_index];
  }

  size_t term_count(size_t index, size_t search_hit_index) const override {
    return 1;
  }

 private:
  Index::PositionalList::const_iterator positional_list_iter(
      size_t index) const {
    auto it = positional_list_.begin();
    std::advance(it, index);
    return it;
  }

  const Index &index_;
  const Index::PositionalList &positional_list_;
};

static std::shared_ptr<SearchResult> perform_term_operation(
    const Index &index, const Expression &expr) {
  return std::make_shared<SearchResultForTermOperation>(index, expr.term_id);
}

std::shared_ptr<SearchResult> perform_search(const Index &index,
                                             const Expression &expr) {
  switch (expr.operation) {
    case Operation::Term:
      return perform_term_operation(index, expr);
    default:
      return nullptr;
  }
}

}  // namespace searchlib
