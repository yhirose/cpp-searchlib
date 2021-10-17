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

class TermSearchResult : public IPostings {
 public:
  TermSearchResult(const IInvertedIndex &inverted_index,
                   const std::u32string &str)
      : postings_(inverted_index.postings(str)) {}

  ~TermSearchResult() override = default;

  size_t size() const override { return postings_.size(); }

  size_t document_id(size_t index) const override {
    return postings_.document_id(index);
  }

  size_t search_hit_count(size_t index) const override {
    return postings_.search_hit_count(index);
  }

  size_t term_position(size_t index, size_t search_hit_index) const override {
    return postings_.term_position(index, search_hit_index);
  }

  size_t term_length(size_t index, size_t search_hit_index) const override {
    return 1;
  }

  bool is_term_position(size_t index, size_t term_pos) const override {
    return postings_.is_term_position(index, term_pos);
  }

 private:
  const IPostings &postings_;
};

//-----------------------------------------------------------------------------

class Position {
 public:
  Position(size_t document_id, std::vector<size_t> &&term_positions,
           std::vector<size_t> &&term_lengths)
      : document_id_(document_id),
        term_positions_(term_positions),
        term_lengths(term_lengths) {}

  ~Position() = default;

  size_t document_id() const { return document_id_; }

  size_t search_hit_count() const { return term_positions_.size(); }

  size_t term_position(size_t search_hit_index) const {
    return term_positions_[search_hit_index];
  }

  size_t term_length(size_t search_hit_index) const {
    return term_lengths[search_hit_index];
  }

  bool is_term_position(size_t term_pos) const {
    return std::binary_search(term_positions_.begin(), term_positions_.end(),
                              term_pos);
  }

 private:
  size_t document_id_;
  std::vector<size_t> term_positions_;
  std::vector<size_t> term_lengths;
};

class SearchResult : public IPostings {
 public:
  ~SearchResult() override = default;

  size_t size() const override { return positions_.size(); }

  size_t document_id(size_t index) const override {
    return positions_[index]->document_id();
  }

  size_t search_hit_count(size_t index) const override {
    return positions_[index]->search_hit_count();
  }

  size_t term_position(size_t index, size_t search_hit_index) const override {
    return positions_[index]->term_position(search_hit_index);
  }

  size_t term_length(size_t index, size_t search_hit_index) const override {
    return positions_[index]->term_length(search_hit_index);
  }

  bool is_term_position(size_t index, size_t term_pos) const override {
    return positions_[index]->is_term_position(term_pos);
  }

  void push_back(std::shared_ptr<Position> info) { positions_.push_back(std::move(info)); }

 private:
  std::vector<std::shared_ptr<Position>> positions_;
};

//-----------------------------------------------------------------------------

static auto positings_list(const IInvertedIndex &inverted_index,
                           const std::vector<Expression> &nodes) {
  std::vector<std::shared_ptr<IPostings>> positings_list;
  for (const auto &expr : nodes) {
    positings_list.push_back(perform_search(inverted_index, expr));
  }
  return positings_list;
}

static std::vector<size_t /*slot*/> min_slots(
    const std::vector<std::shared_ptr<IPostings>> &positings_list,
    const std::vector<size_t> &cursors) {
  std::vector<size_t> slots = {0};

  for (size_t slot = 1; slot < positings_list.size(); slot++) {
    auto prev = positings_list[slots[0]]->document_id(cursors[slots[0]]);
    auto curr = positings_list[slot]->document_id(cursors[slot]);

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
    const std::vector<std::shared_ptr<IPostings>> &positings_list,
    const std::vector<size_t> &cursors) {
  auto min = positings_list[0]->document_id(cursors[0]);
  auto max = min;

  for (size_t slot = 1; slot < positings_list.size(); slot++) {
    auto id = positings_list[slot]->document_id(cursors[slot]);
    if (id < min) {
      min = id;
    } else if (id > max) {
      max = id;
    }
  }

  return std::make_pair(min, max);
}

static bool skip_cursors(
    const std::vector<std::shared_ptr<IPostings>> &positings_list,
    std::vector<size_t> &cursors, size_t document_id) {
  for (size_t slot = 0; slot < positings_list.size(); slot++) {
    // TODO: skip list support
    while (cursors[slot] < positings_list[slot]->size()) {
      if (positings_list[slot]->document_id(cursors[slot]) >= document_id) {
        break;
      }
      cursors[slot]++;
    }

    if (cursors[slot] == positings_list[slot]->size()) {
      return true;
    }
  }
  return false;
}

static bool increment_all_cursors(
    const std::vector<std::shared_ptr<IPostings>> &positings_list,
    std::vector<size_t> &cursors) {
  for (size_t slot = 0; slot < positings_list.size(); slot++) {
    cursors[slot]++;
    if (cursors[slot] == positings_list[slot]->size()) {
      return true;
    }
  }
  return false;
}

static void increment_cursors(
    std::vector<std::shared_ptr<IPostings>> &positings_list,
    std::vector<size_t> &cursors, const std::vector<size_t> &slots) {
  for (int i = slots.size() - 1; i >= 0; i--) {
    auto slot = slots[i];
    cursors[slot]++;
    if (cursors[slot] == positings_list[slot]->size()) {
      cursors.erase(cursors.begin() + slot);
      positings_list.erase(positings_list.begin() + slot);
    }
  }
}

static size_t shortest_slot(
    const std::vector<std::shared_ptr<IPostings>> &positings_list,
    const std::vector<size_t> &cursors) {
  size_t shortest_slot = 0;
  auto shortest_count =
      positings_list[shortest_slot]->search_hit_count(cursors[shortest_slot]);
  for (size_t slot = 1; slot < positings_list.size(); slot++) {
    auto count = positings_list[slot]->search_hit_count(cursors[slot]);
    if (count < shortest_count) {
      shortest_slot = slot;
      shortest_count = count;
    }
  }
  return shortest_slot;
}

static bool is_adjacent(
    const std::vector<std::shared_ptr<IPostings>> &positings_list,
    const std::vector<size_t> &cursors, size_t target_slot, size_t term_pos) {
  auto ret = true;

  for (size_t slot = 0; ret && slot < positings_list.size(); slot++) {
    if (slot == target_slot) {
      continue;
    }

    auto delta = slot - target_slot;
    auto next_term_pos = term_pos + delta;
    ret = positings_list[slot]->is_term_position(cursors[slot], next_term_pos);
  }

  return ret;
}

template <typename T>
static std::shared_ptr<IPostings> intersect_postings(
    const std::vector<std::shared_ptr<IPostings>> &positings_list,
    T make_positions) {
  auto result = std::make_shared<SearchResult>();
  std::vector<size_t> cursors(positings_list.size(), 0);

  auto done = false;
  while (!done) {
    auto [min, max] = min_max_slots(positings_list, cursors);
    if (min == max) {
      auto positions = make_positions(positings_list, cursors);
      if (positions) {
        result->push_back(positions);
      }
      done = increment_all_cursors(positings_list, cursors);
    } else {
      done = skip_cursors(positings_list, cursors, max);
    }
  }

  return result;
}

static void merge_term_positions(
    const std::vector<std::shared_ptr<IPostings>> &positings_list,
    const std::vector<size_t> &cursors, const std::vector<size_t> &slots,
    std::vector<size_t> &term_positions, std::vector<size_t> &term_lengths) {
  std::vector<size_t> search_hit_cursors(positings_list.size(), 0);

  while (true) {
    size_t min_slot = -1;
    size_t min_term_pos = -1;
    size_t min_term_length = -1;

    // TODO: improve performance by reducing slots
    for (auto slot : slots) {
      auto index = cursors[slot];
      auto p = positings_list[slot];
      auto hit_index = search_hit_cursors[slot];

      if (hit_index < p->search_hit_count(index)) {
        auto term_pos = p->term_position(index, hit_index);
        auto term_length = p->term_length(index, hit_index);

        if (term_pos < min_term_pos) {
          min_slot = slot;
          min_term_pos = term_pos;
          min_term_length = term_length;
        }
      }
    }

    if (min_slot == -1) {
      break;
    }

    term_positions.push_back(min_term_pos);
    term_lengths.push_back(min_term_length);
    search_hit_cursors[min_slot]++;
  }
}

static std::shared_ptr<IPostings> union_postings(
    std::vector<std::shared_ptr<IPostings>> &&positings_list) {
  auto result = std::make_shared<SearchResult>();
  std::vector<size_t> cursors(positings_list.size(), 0);

  while (!positings_list.empty()) {
    auto slots = min_slots(positings_list, cursors);

    std::vector<size_t> term_positions;
    std::vector<size_t> term_lengths;
    merge_term_positions(positings_list, cursors, slots, term_positions,
                         term_lengths);

    result->push_back(std::make_shared<Position>(
        positings_list[slots[0]]->document_id(cursors[slots[0]]),
        std::move(term_positions), std::move(term_lengths)));

    increment_cursors(positings_list, cursors, slots);
    assert(positings_list.size() == cursors.size());
  }

  return result;
}

//-----------------------------------------------------------------------------

static std::shared_ptr<IPostings> perform_term_operation(
    const IInvertedIndex &inverted_index, const Expression &expr) {
  return std::make_shared<TermSearchResult>(inverted_index, expr.term_str);
}

static std::shared_ptr<IPostings> perform_and_operation(
    const IInvertedIndex &inverted_index, const Expression &expr) {
  return intersect_postings(
      positings_list(inverted_index, expr.nodes),
      [](const auto &positings_list, const auto &cursors) {
        std::vector<size_t> slots(positings_list.size(), 0);
        std::iota(slots.begin(), slots.end(), 0);

        std::vector<size_t> term_positions;
        std::vector<size_t> term_lengths;
        merge_term_positions(positings_list, cursors, slots, term_positions,
                             term_lengths);

        return std::make_shared<Position>(
            positings_list[0]->document_id(cursors[0]),
            std::move(term_positions), std::move(term_lengths));
      });
}

static std::shared_ptr<IPostings> perform_adjacent_operation(
    const IInvertedIndex &inverted_index, const Expression &expr) {
  return intersect_postings(
      positings_list(inverted_index, expr.nodes),
      [](const auto &positings_list, const auto &cursors) {
        std::vector<size_t> term_positions;
        std::vector<size_t> term_lengths;

        auto target_slot = shortest_slot(positings_list, cursors);

        auto count =
            positings_list[target_slot]->search_hit_count(cursors[target_slot]);

        for (size_t i = 0; i < count; i++) {
          auto term_pos = positings_list[target_slot]->term_position(
              cursors[target_slot], i);
          if (is_adjacent(positings_list, cursors, target_slot, term_pos)) {
            auto start_term_pos = term_pos - target_slot;
            term_positions.push_back(start_term_pos);
            term_lengths.push_back(positings_list.size());
          }
        }

        if (term_positions.empty()) {
          return std::shared_ptr<Position>();
        } else {
          return std::make_shared<Position>(
              positings_list[0]->document_id(cursors[0]),
              std::move(term_positions), std::move(term_lengths));
        }
      });
}

static std::shared_ptr<IPostings> perform_or_operation(
    const IInvertedIndex &inverted_index, const Expression &expr) {
  return union_postings(positings_list(inverted_index, expr.nodes));
}

static std::shared_ptr<IPostings> perform_near_operation(
    const IInvertedIndex &inverted_index, const Expression &expr) {
  return intersect_postings(
      positings_list(inverted_index, expr.nodes),
      [&](const auto &positings_list, const auto &cursors) {
        std::vector<size_t> term_positions;
        std::vector<size_t> term_lengths;
        std::vector<size_t> search_hit_cursors(positings_list.size(), 0);

        auto done = false;
        while (!done) {
          // TODO: performance improvement by reusing values as many as
          // possible
          std::map<size_t /*term_pos*/,
                   std::pair<size_t /*slot*/, size_t /*term_length*/>>
              slots_by_term_pos;
          {
            auto slot = 0;
            for (const auto &p : positings_list) {
              auto index = cursors[slot];
              auto hit_index = search_hit_cursors[slot];
              auto term_pos = p->term_position(index, hit_index);
              auto term_length = p->term_length(index, hit_index);
              slots_by_term_pos[term_pos] = std::pair(slot, term_length);
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
              if (delta > expr.near_operation_distance) {
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
              auto [slot, term_length] = item;
              term_positions.push_back(term_pos);
              term_lengths.push_back(term_length);
              search_hit_cursors[slot]++;

              if (search_hit_cursors[slot] ==
                  positings_list[slot]->search_hit_count(cursors[slot])) {
                done = true;
              }
            }
          } else {
            // Skip search hit cursor for the smallest slot
            auto slot = slots_by_term_pos.begin()->second.first;
            search_hit_cursors[slot]++;

            if (search_hit_cursors[slot] ==
                positings_list[slot]->search_hit_count(cursors[slot])) {
              done = true;
            }
          }
        }

        if (term_positions.empty()) {
          return std::shared_ptr<Position>();
        } else {
          return std::make_shared<Position>(
              positings_list[0]->document_id(cursors[0]),
              std::move(term_positions), std::move(term_lengths));
        }
      });
}

//-----------------------------------------------------------------------------

std::shared_ptr<IPostings> perform_search(const IInvertedIndex &inverted_index,
                                          const Expression &expr) {
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

template <typename T>
void enumerate_terms(const Expression &expr, T fn) {
  if (expr.operation == Operation::Term) {
    fn(expr.term_str);
  } else {
    for (const auto &node : expr.nodes) {
      enumerate_terms(node, fn);
    }
  }
}

size_t term_count_score(const IInvertedIndex &invidx, const Expression &expr,
                        const IPostings &postings, size_t index) {
  auto document_id = postings.document_id(index);
  size_t score = 0;
  enumerate_terms(expr, [&](const auto &term) {
    score += invidx.term_count(term, document_id);
  });
  return score;
}

double tf_idf_score(const IInvertedIndex &invidx, const Expression &expr,
                    const IPostings &postings, size_t index) {
  auto document_id = postings.document_id(index);
  auto N = static_cast<double>(invidx.document_count());
  double score = 0.0;
  enumerate_terms(expr, [&](const auto &term) {
    auto n = static_cast<double>(invidx.df(term));
    auto idf = std::log2((N + 0.001) / (n + 0.001));
    score += invidx.tf(term, document_id) * idf;
  });
  return score;
}

double bm25_score(const IInvertedIndex &invidx, const Expression &expr,
                  const IPostings &postings, size_t index, double k1,
                  double b) {
  auto document_id = postings.document_id(index);
  auto N = static_cast<double>(invidx.document_count());
  auto dl = static_cast<double>(invidx.document_term_count(document_id));
  auto avgdl = static_cast<double>(invidx.average_document_term_count());

  double score = 0.0;
  enumerate_terms(expr, [&](const auto &term) {
    auto n = static_cast<double>(invidx.df(term));
    auto idf = std::log2((N - n + 0.5) / (n + 0.5));
    auto tf = invidx.tf(term, document_id);

    score +=
        idf * ((tf * (k1 + 1.0)) / (tf + k1 * (1.0 - b + b * (dl / avgdl))));
  });
  return score;
}

}  // namespace searchlib
