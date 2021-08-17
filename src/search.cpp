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

class TermSearchResult : public SearchResult {
 public:
  TermSearchResult(const InvertedIndex &inverted_index, size_t term_id)
      : inverted_index_(inverted_index),
        positional_list_(inverted_index.posting_list.at(term_id)) {}

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

  bool has_term_pos(size_t index, size_t term_pos) const override {
    // TODO: binary search
    for (const auto &pos : positional_list_iter(index)->second) {
      if (pos == term_pos) {
        return true;
      }
    }
    return false;
  }

 private:
  PositionalList::const_iterator positional_list_iter(size_t index) const {
    auto it = positional_list_.begin();
    std::advance(it, index);
    return it;
  }

  const InvertedIndex &inverted_index_;
  const PositionalList &positional_list_;
};

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

  bool has_term_pos(size_t term_pos) const {
    // TODO: binary search
    for (size_t hit_index = 0; hit_index < term_positional_info_.size();
         hit_index++) {
      if (term_position(hit_index) == term_pos) {
        return true;
      }
    }
    return false;
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

  bool has_term_pos(size_t index, size_t term_pos) const override {
    return items_[index]->has_term_pos(term_pos);
  }

  void push_back(std::shared_ptr<PositionalInfo> info) {
    items_.push_back(info);
  }

 private:
  std::vector<std::shared_ptr<PositionalInfo>> items_;
};

//-----------------------------------------------------------------------------

static auto postings(const InvertedIndex &inverted_index,
                     const std::vector<Expression> &nodes) {
  std::vector<std::shared_ptr<SearchResult>> results;
  for (const auto &expr : nodes) {
    results.push_back(perform_search(inverted_index, expr));
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

static size_t shortest_slot(
    const std::vector<std::shared_ptr<SearchResult>> &postings,
    const std::vector<size_t> &cursors) {
  size_t shortest_slot = 0;
  auto shortest_count =
      postings[shortest_slot]->search_hit_count(cursors[shortest_slot]);
  for (size_t slot = 1; slot < postings.size(); slot++) {
    auto count = postings[slot]->search_hit_count(cursors[slot]);
    if (count < shortest_count) {
      shortest_slot = slot;
      shortest_count = count;
    }
  }
  return shortest_slot;
}

static bool is_adjacent(
    const std::vector<std::shared_ptr<SearchResult>> &postings,
    const std::vector<size_t> &cursors, size_t target_slot, size_t term_pos) {
  auto ret = true;

  for (size_t slot = 0; ret && slot < postings.size(); slot++) {
    if (slot == target_slot) {
      continue;
    }

    auto delta = slot - target_slot;
    auto next_term_pos = term_pos + delta;
    ret = postings[slot]->has_term_pos(cursors[slot], next_term_pos);
  }

  return ret;
}

template <typename T>
static std::shared_ptr<SearchResult> intersect_postings(
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
    std::vector<std::shared_ptr<SearchResult>> &&postings) {
  auto result = std::make_shared<TempSearchResult>();
  std::vector<size_t> cursors(postings.size(), 0);

  while (!postings.empty()) {
    auto slots = min_slots(postings, cursors);
    auto doc_id = postings[slots[0]]->document_id(cursors[slots[0]]);

    std::vector<std::pair<size_t, size_t>> term_positional_info;
    {
      // TODO: merge instead of sort
      for (auto slot : slots) {
        auto index = cursors[slot];
        auto p = postings[slot];
        auto hit_count = p->search_hit_count(index);

        for (size_t hit_index = 0; hit_index < hit_count; hit_index++) {
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

static std::shared_ptr<SearchResult> perform_term_operation(
    const InvertedIndex &inverted_index, const Expression &expr) {
  return std::make_shared<TermSearchResult>(inverted_index, expr.term_id);
}

static std::shared_ptr<SearchResult> perform_and_operation(
    const InvertedIndex &inverted_index, const Expression &expr) {
  return intersect_postings(
      postings(inverted_index, expr.nodes),
      [](const auto &postings, const auto &cursors) {
        std::vector<std::pair<size_t, size_t>> term_positional_info;

        // TODO: merge instead of sort
        auto slot = 0;
        for (const auto &p : postings) {
          auto hit_count = p->search_hit_count(cursors[slot]);
          for (size_t hit_index = 0; hit_index < hit_count; hit_index++) {
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
    const InvertedIndex &inverted_index, const Expression &expr) {
  return intersect_postings(
      postings(inverted_index, expr.nodes),
      [](const auto &postings, const auto &cursors) {
        std::vector<std::pair<size_t, size_t>> term_positional_info;

        auto target_slot = shortest_slot(postings, cursors);

        auto count =
            postings[target_slot]->search_hit_count(cursors[target_slot]);
        for (size_t i = 0; i < count; i++) {
          auto term_pos =
              postings[target_slot]->term_position(cursors[target_slot], i);
          if (is_adjacent(postings, cursors, target_slot, term_pos)) {
            auto start_term_pos = term_pos - target_slot;
            term_positional_info.emplace_back(start_term_pos, postings.size());
          }
        }

        if (term_positional_info.empty()) {
          return std::shared_ptr<PositionalInfo>();
        } else {
          return std::make_shared<PositionalInfo>(
              postings[0]->document_id(cursors[0]),
              std::move(term_positional_info));
        }
      });
}

static std::shared_ptr<SearchResult> perform_or_operation(
    const InvertedIndex &inverted_index, const Expression &expr) {
  return union_postings(postings(inverted_index, expr.nodes));
}

static std::shared_ptr<SearchResult> perform_near_operation(
    const InvertedIndex &inverted_index, const Expression &expr) {
  return intersect_postings(
      postings(inverted_index, expr.nodes),
      [&](const auto &postings, const auto &cursors) {
        std::vector<std::pair<size_t, size_t>> term_positional_info;
        std::vector<size_t> search_hit_cursors(postings.size(), 0);

        auto done = false;
        while (!done) {
          std::map<size_t /*term_pos*/,
                   std::pair<size_t /*slot*/, size_t /*term_count*/>>
              slots_by_term_pos;
          {
            auto slot = 0;
            for (const auto &p : postings) {
              auto index = cursors[slot];
              auto hit_index = search_hit_cursors[slot];
              auto term_pos = p->term_position(index, hit_index);
              auto term_count = p->term_count(index, hit_index);
              slots_by_term_pos[term_pos] = std::pair(slot, term_count);
              slot++;
            }
          }

          auto near = true;
          {
            auto it = slots_by_term_pos.begin();
            auto it_prev = it;
            ++it;
            while (it != slots_by_term_pos.end()) {
              auto [prev_term_pos, prev_item] = *it_prev;
              auto [prev_slot, prev_term_count] = prev_item;
              auto [term_pos, item] = *it;
              auto delta = term_pos - (prev_term_pos + prev_term_count - 1);
              if (delta > expr.near_size) {
                near = false;
                break;
              }
              it_prev = it;
              ++it;
            }
          }

          if (near) {
            // Skip all search hit cursors
            for (auto [term_pos, item] : slots_by_term_pos) {
              auto [slot, term_count] = item;
              term_positional_info.emplace_back(term_pos, term_count);
              search_hit_cursors[slot]++;
              if (search_hit_cursors[slot] ==
                  postings[slot]->search_hit_count(cursors[slot])) {
                done = true;
              }
            }
          } else {
            // Skip search hit cursor for the smallest slot
            auto slot = slots_by_term_pos.begin()->second.first;
            search_hit_cursors[slot]++;
            if (search_hit_cursors[slot] ==
                postings[slot]->search_hit_count(cursors[slot])) {
              done = true;
            }
          }
        }

        if (term_positional_info.empty()) {
          return std::shared_ptr<PositionalInfo>();
        } else {
          return std::make_shared<PositionalInfo>(
              postings[0]->document_id(cursors[0]),
              std::move(term_positional_info));
        }
      });
}

//-----------------------------------------------------------------------------

std::shared_ptr<SearchResult> perform_search(
    const InvertedIndex &inverted_index, const Expression &expr) {
  switch (expr.operation) {
    case Operation::Term:
      return perform_term_operation(inverted_index, expr);
    case Operation::And:
      return perform_and_operation(inverted_index, expr);
    case Operation::Adjacent:
      return perform_adjacent_operation(inverted_index, expr);
    case Operation::Or:
      return perform_or_operation(inverted_index, expr);
    case Operation::Near:
      return perform_near_operation(inverted_index, expr);
    default:
      return nullptr;
  }
}

}  // namespace searchlib
