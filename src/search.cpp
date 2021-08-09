//
//  search.cpp
//
//  Copyright (c) 2021 Yuji Hirose. All rights reserved.
//  MIT License
//

#include <array>
#include <iostream>
#include <numeric>

#include "./utils.h"
#include "searchlib.h"

std::ostream &operator<<(std::ostream &os, const std::vector<size_t> &v) {
  size_t i = 0;
  for (auto x : v) {
    if (i++ > 0) {
      os << ',';
    }
    os << x;
  }
  return os;
}

namespace searchlib {

SearchResult::~SearchResult() = default;

//-----------------------------------------------------------------------------

class PositionalInfo {
 public:
  PositionalInfo(size_t document_id,
                 std::vector<std::pair<size_t /*position*/, size_t /*count*/>>
                     &&term_positional_info)
      : document_id_(document_id),
        term_positional_info_(term_positional_info) {}

  ~PositionalInfo() = default;

  size_t document_id() const { return document_id_; }

  size_t search_hit_count() const { return term_positional_info_.size(); }

  size_t term_position(size_t search_hit_index) const {
    return term_positional_info_[search_hit_index].first;
  }

  size_t term_count(size_t search_hit_index) const {
    return term_positional_info_[search_hit_index].second;
  }

 private:
  size_t document_id_;
  std::vector<std::pair<size_t, size_t>> term_positional_info_;
};

class TempSearchResult : public SearchResult {
 public:
  ~TempSearchResult() override = default;

  size_t size() const override { return items_.size(); }

  size_t document_id(size_t index) const override {
    return items_[index]->document_id();
  }

  size_t search_hit_count(size_t index) const override {
    return items_[index]->search_hit_count();
  }

  size_t term_position(size_t index, size_t search_hit_index) const override {
    return items_[index]->term_position(search_hit_index);
  }

  size_t term_count(size_t index, size_t search_hit_index) const override {
    return items_[index]->term_count(search_hit_index);
  }

  void push_back(std::shared_ptr<PositionalInfo> info) {
    items_.push_back(info);
  }

 private:
  std::vector<std::shared_ptr<PositionalInfo>> items_;
};

//-----------------------------------------------------------------------------

static auto postings(const Index &index, const std::vector<Expression> &nodes) {
  std::vector<std::shared_ptr<SearchResult>> results;
  for (const auto &expr : nodes) {
    results.push_back(perform_search(index, expr));
  }
  return results;
}

static std::vector<size_t /*slot*/> min_slots(
    const std::vector<std::shared_ptr<SearchResult>> &postings,
    const std::vector<size_t> &cursors) {
  std::vector<size_t> slots = {0};

  for (size_t slot = 1; slot < postings.size(); slot++) {
    auto prev = postings[slots[0]]->document_id(cursors[slots[0]]);
    auto curr = postings[slot]->document_id(cursors[slot]);

    if (curr < prev) {
      slots.clear();
      slots.push_back(slot);
    } else if (curr == prev) {
      slots.push_back(slot);
    }
  }

  return slots;
}

static std::pair<size_t /*min*/, size_t /*max*/> min_max_slots(
    const std::vector<std::shared_ptr<SearchResult>> &postings,
    const std::vector<size_t> &cursors) {
  auto min = postings[0]->document_id(cursors[0]);
  auto max = min;

  for (size_t slot = 1; slot < postings.size(); slot++) {
    auto id = postings[slot]->document_id(cursors[slot]);
    if (id < min) {
      min = id;
    } else if (id > max) {
      max = id;
    }
  }

  return std::make_pair(min, max);
}

static bool skip_cursors(
    const std::vector<std::shared_ptr<SearchResult>> &postings,
    std::vector<size_t> &cursors, size_t document_id) {
  for (int slot = postings.size() - 1; slot >= 0; slot--) {
    // TODO: skip list suport
    while (cursors[slot] < postings[slot]->size()) {
      if (postings[slot]->document_id(cursors[slot]) >= document_id) {
        break;
      }
      cursors[slot]++;
    }

    if (cursors[slot] == postings[slot]->size()) {
      return true;
    }
  }
  return false;
}

static bool increment_all_cursors(
    const std::vector<std::shared_ptr<SearchResult>> &postings,
    std::vector<size_t> &cursors) {
  for (int slot = postings.size() - 1; slot >= 0; slot--) {
    cursors[slot]++;
    if (cursors[slot] == postings[slot]->size()) {
      return true;
    }
  }
  return false;
}

static void increment_cursors(
    std::vector<std::shared_ptr<SearchResult>> &postings,
    std::vector<size_t> &cursors, const std::vector<size_t> &slots) {
  for (int i = slots.size() - 1; i >= 0; i--) {
    auto slot = slots[i];
    cursors[slot]++;
    if (cursors[slot] == postings[slot]->size()) {
      cursors.erase(cursors.begin() + slot);
      postings.erase(postings.begin() + slot);
    }
  }
}

template <typename T>
static std::shared_ptr<SearchResult> intersect_postings(
    const Index &index,
    const std::vector<std::shared_ptr<SearchResult>> &postings,
    T make_positional_list) {
  auto result = std::make_shared<TempSearchResult>();
  std::vector<size_t> cursors(postings.size(), 0);

  auto done = false;
  while (!done) {
    auto [min, max] = min_max_slots(postings, cursors);
    if (min == max) {
      auto positional = make_positional_list(postings, cursors);
      if (positional) {
        result->push_back(positional);
      }
      done = increment_all_cursors(postings, cursors);
    } else {
      done = skip_cursors(postings, cursors, max);
    }
  }

  return result;
}

static std::shared_ptr<SearchResult> union_postings(
    const Index &index, std::vector<std::shared_ptr<SearchResult>> &&postings) {
  auto result = std::make_shared<TempSearchResult>();
  std::vector<size_t> cursors(postings.size(), 0);

  while (!postings.empty()) {
    auto slots = min_slots(postings, cursors);
    auto doc_id = postings[slots[0]]->document_id(cursors[slots[0]]);

    std::vector<std::pair<size_t, size_t>> term_positional_info;
    {
      for (auto slot : slots) {
        auto index = cursors[slot];
        auto p = postings[slot];

        for (size_t hit_index = 0; hit_index < p->search_hit_count(index);
             hit_index++) {
          term_positional_info.emplace_back(p->term_position(index, hit_index),
                                            p->term_count(index, hit_index));
        }
      }

      std::sort(term_positional_info.begin(), term_positional_info.end(),
                [](auto a, auto b) { return a.first < b.first; });
    }

    result->push_back(std::make_shared<PositionalInfo>(
        doc_id, std::move(term_positional_info)));

    increment_cursors(postings, cursors, slots);
    assert(postings.size() == cursors.size());
  }

  return result;
}

//-----------------------------------------------------------------------------

class TermSearchResult : public SearchResult {
 public:
  TermSearchResult(const Index &index, size_t term_id)
      : index_(index), positional_list_(index.posting_list.at(term_id)) {}

  ~TermSearchResult() override = default;

  size_t size() const override { return positional_list_.size(); }

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
  PositionalList::const_iterator positional_list_iter(size_t index) const {
    auto it = positional_list_.begin();
    std::advance(it, index);
    return it;
  }

  const Index &index_;
  const PositionalList &positional_list_;
};

static std::shared_ptr<SearchResult> perform_term_operation(const Index &index,
                                                            size_t term_id) {
  return std::make_shared<TermSearchResult>(index, term_id);
}

//-----------------------------------------------------------------------------

static std::shared_ptr<SearchResult> perform_and_operation(
    const Index &index, const std::vector<Expression> &nodes) {
  return intersect_postings(
      index, postings(index, nodes),
      [](const auto &postings, const auto &cursors) {
        std::vector<std::pair<size_t, size_t>> term_positional_info;

        auto slot = 0;
        for (const auto &p : postings) {
          for (size_t hit_index = 0;
               hit_index < p->search_hit_count(cursors[slot]); hit_index++) {
            term_positional_info.emplace_back(
                p->term_position(cursors[slot], hit_index),
                p->term_count(cursors[slot], hit_index));
          }
          slot++;
        }

        std::sort(term_positional_info.begin(), term_positional_info.end(),
                  [](auto a, auto b) { return a.first < b.first; });

        return std::make_shared<PositionalInfo>(
            postings[0]->document_id(cursors[0]),
            std::move(term_positional_info));
      });
}

static std::shared_ptr<SearchResult> perform_adjacent_operation(
    const Index &index, const std::vector<Expression> &nodes) {
  return perform_and_operation(index, nodes);
}

static std::shared_ptr<SearchResult> perform_or_operation(
    const Index &index, const std::vector<Expression> &nodes) {
  return union_postings(index, postings(index, nodes));
}

//-----------------------------------------------------------------------------

std::shared_ptr<SearchResult> perform_search(const Index &index,
                                             const Expression &expr) {
  switch (expr.operation) {
    case Operation::Term:
      return perform_term_operation(index, expr.term_id);
    case Operation::And:
      return perform_and_operation(index, expr.nodes);
    case Operation::Adjacent:
      return perform_adjacent_operation(index, expr.nodes);
    case Operation::Or:
      return perform_or_operation(index, expr.nodes);
    default:
      return nullptr;
  }
}

}  // namespace searchlib
