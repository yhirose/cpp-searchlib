//
//  search.cpp
//
//  Copyright (c) 2021 Yuji Hirose. All rights reserved.
//  MIT License
//

#include <array>
#include <cassert>
#include <iostream>
#include <numeric>

#include "./utils.h"
#include "searchlib.h"

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
    const auto &positions = positional_list_iter(index)->second;
    return std::binary_search(positions.begin(), positions.end(), term_pos);
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
  PositionalInfo(size_t document_id, std::vector<size_t> &&term_positions,
                 std::vector<size_t> &&term_counts)
      : document_id_(document_id),
        term_positions_(term_positions),
        term_counts_(term_counts) {}

  ~PositionalInfo() = default;

  size_t document_id() const { return document_id_; }

  size_t search_hit_count() const { return term_positions_.size(); }

  size_t term_position(size_t search_hit_index) const {
    return term_positions_[search_hit_index];
  }

  size_t term_count(size_t search_hit_index) const {
    return term_counts_[search_hit_index];
  }

  bool has_term_pos(size_t term_pos) const {
    return std::binary_search(term_positions_.begin(), term_positions_.end(), term_pos);
  }

 private:
  size_t document_id_;
  std::vector<size_t> term_positions_;
  std::vector<size_t> term_counts_;
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
  for (size_t slot = 0; slot < postings.size(); slot++) {
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
  for (size_t slot = 0; slot < postings.size(); slot++) {
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

static void merge_term_positional_info(
    const std::vector<std::shared_ptr<SearchResult>> &postings,
    const std::vector<size_t> &cursors, const std::vector<size_t> &slots,
    std::vector<size_t> &term_positions, std::vector<size_t> &term_counts) {
  std::vector<size_t> search_hit_cursors(postings.size(), 0);

  while (true) {
    size_t min_slot = -1;
    size_t min_term_pos = -1;
    size_t min_term_count = -1;

    // TODO: improve performance by reducing slots
    for (auto slot : slots) {
      auto index = cursors[slot];
      auto p = postings[slot];
      auto hit_index = search_hit_cursors[slot];

      if (hit_index < p->search_hit_count(index)) {
        auto term_pos = p->term_position(index, hit_index);
        auto term_count = p->term_count(index, hit_index);

        if (term_pos < min_term_pos) {
          min_slot = slot;
          min_term_pos = term_pos;
          min_term_count = term_count;
        }
      }
    }

    if (min_slot == -1) {
      break;
    }

    term_positions.push_back(min_term_pos);
    term_counts.push_back(min_term_count);
    search_hit_cursors[min_slot]++;
  }
}

static std::shared_ptr<SearchResult> union_postings(
    std::vector<std::shared_ptr<SearchResult>> &&postings) {
  auto result = std::make_shared<TempSearchResult>();
  std::vector<size_t> cursors(postings.size(), 0);

  while (!postings.empty()) {
    auto slots = min_slots(postings, cursors);

    std::vector<size_t> term_positions;
    std::vector<size_t> term_counts;
    merge_term_positional_info(postings, cursors, slots, term_positions,
                               term_counts);

    result->push_back(std::make_shared<PositionalInfo>(
        postings[slots[0]]->document_id(cursors[slots[0]]),
        std::move(term_positions), std::move(term_counts)));

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
        std::vector<size_t> slots(postings.size(), 0);
        std::iota(slots.begin(), slots.end(), 0);

        std::vector<size_t> term_positions;
        std::vector<size_t> term_counts;
        merge_term_positional_info(postings, cursors, slots, term_positions,
                                   term_counts);

        return std::make_shared<PositionalInfo>(
            postings[0]->document_id(cursors[0]), std::move(term_positions),
            std::move(term_counts));
      });
}

static std::shared_ptr<SearchResult> perform_adjacent_operation(
    const InvertedIndex &inverted_index, const Expression &expr) {
  return intersect_postings(
      postings(inverted_index, expr.nodes),
      [](const auto &postings, const auto &cursors) {
        std::vector<size_t> term_positions;
        std::vector<size_t> term_counts;

        auto target_slot = shortest_slot(postings, cursors);

        auto count =
            postings[target_slot]->search_hit_count(cursors[target_slot]);

        for (size_t i = 0; i < count; i++) {
          auto term_pos =
              postings[target_slot]->term_position(cursors[target_slot], i);
          if (is_adjacent(postings, cursors, target_slot, term_pos)) {
            auto start_term_pos = term_pos - target_slot;
            term_positions.push_back(start_term_pos);
            term_counts.push_back(postings.size());
          }
        }

        if (term_positions.empty()) {
          return std::shared_ptr<PositionalInfo>();
        } else {
          return std::make_shared<PositionalInfo>(
              postings[0]->document_id(cursors[0]), std::move(term_positions),
              std::move(term_counts));
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
        std::vector<size_t> term_positions;
        std::vector<size_t> term_counts;
        std::vector<size_t> search_hit_cursors(postings.size(), 0);

        auto done = false;
        while (!done) {
          // TODO: performance improvement by resusing values as many as
          // possible
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
              term_positions.push_back(term_pos);
              term_counts.push_back(term_count);
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

        if (term_positions.empty()) {
          return std::shared_ptr<PositionalInfo>();
        } else {
          return std::make_shared<PositionalInfo>(
              postings[0]->document_id(cursors[0]), std::move(term_positions),
              std::move(term_counts));
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
