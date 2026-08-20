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

// Defined in invertedindex.cpp; shared here because BM25Scorer locates a
// document inside a term's postings the same way tf() does.
size_t find_postings_index_for_document_id_(const IPostings &p,
                                            size_t document_id);

class TermSearchResult : public IPostings {
public:
  TermSearchResult(const IInvertedIndex &inverted_index,
                   const std::u32string &str)
      : postings_(inverted_index.postings(str)) {}

  ~TermSearchResult() override = default;

  size_t size() const override { return postings_->size(); }

  size_t document_id(size_t index) const override {
    return postings_->document_id(index);
  }

  size_t search_hit_count(size_t index) const override {
    return postings_->search_hit_count(index);
  }

  size_t term_position(size_t index, size_t search_hit_index) const override {
    return postings_->term_position(index, search_hit_index);
  }

  size_t term_length(size_t index, size_t search_hit_index) const override {
    return 1;
  }

  bool is_term_position(size_t index, size_t term_pos) const override {
    return postings_->is_term_position(index, term_pos);
  }

private:
  std::shared_ptr<const IPostings> postings_;
};

//-----------------------------------------------------------------------------

// Decorator that hides logically-deleted (tombstoned) documents from a search
// result. It maps external indices to the underlying result's indices, skipping
// any entry whose document_id was removed from the index. Applied once at the
// top of perform_search, so it uniformly covers Term/And/Or/Adjacent/Near.
class FilteredPostings : public IPostings {
public:
  FilteredPostings(const IInvertedIndex &inverted_index,
                   std::shared_ptr<IPostings> postings)
      : postings_(std::move(postings)) {
    auto count = postings_->size();
    live_indices_.reserve(count);
    for (size_t i = 0; i < count; i++) {
      if (!inverted_index.is_document_removed(postings_->document_id(i))) {
        live_indices_.push_back(i);
      }
    }
  }

  ~FilteredPostings() override = default;

  size_t size() const override { return live_indices_.size(); }

  size_t document_id(size_t index) const override {
    return postings_->document_id(live_indices_[index]);
  }

  size_t search_hit_count(size_t index) const override {
    return postings_->search_hit_count(live_indices_[index]);
  }

  size_t term_position(size_t index, size_t search_hit_index) const override {
    return postings_->term_position(live_indices_[index], search_hit_index);
  }

  size_t term_length(size_t index, size_t search_hit_index) const override {
    return postings_->term_length(live_indices_[index], search_hit_index);
  }

  bool is_term_position(size_t index, size_t term_pos) const override {
    return postings_->is_term_position(live_indices_[index], term_pos);
  }

private:
  std::shared_ptr<IPostings> postings_;
  std::vector<size_t> live_indices_;
};

//-----------------------------------------------------------------------------

class Position {
public:
  Position(size_t document_id, std::vector<size_t> &&term_positions,
           std::vector<size_t> &&term_lengths)
      : document_id_(document_id), term_positions_(term_positions),
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

  void push_back(std::shared_ptr<Position> info) {
    positions_.push_back(std::move(info));
  }

private:
  std::vector<std::shared_ptr<Position>> positions_;
};

//-----------------------------------------------------------------------------

// Dispatches an expression to its operation handler without applying the
// tombstone filter. Used internally (including for recursive sub-expressions)
// so that removed documents are filtered exactly once, at the public entry.
// scope_index is threaded through purely so that a nested Operation::SameScope
// node can reach it; every other operation just forwards it unused.
static std::shared_ptr<IPostings>
perform_search_operation(const IInvertedIndex &inverted_index,
                         const Expression &expr,
                         const IScopeIndex *scope_index);

static auto positings_list(const IInvertedIndex &inverted_index,
                           const std::vector<Expression> &nodes,
                           const IScopeIndex *scope_index) {
  std::vector<std::shared_ptr<IPostings>> positings_list;
  for (const auto &expr : nodes) {
    positings_list.push_back(
        perform_search_operation(inverted_index, expr, scope_index));
  }
  return positings_list;
}

static std::vector<size_t /*slot*/>
min_slots(const std::vector<std::shared_ptr<IPostings>> &positings_list,
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

static std::pair<size_t /*min*/, size_t /*max*/>
min_max_slots(const std::vector<std::shared_ptr<IPostings>> &positings_list,
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

static bool
skip_cursors(const std::vector<std::shared_ptr<IPostings>> &positings_list,
             std::vector<size_t> &cursors, size_t document_id) {
  for (size_t slot = 0; slot < positings_list.size(); slot++) {
    const auto &postings = *positings_list[slot];
    auto &cursor = cursors[slot];
    auto size = postings.size();

    // Exponential (galloping) search for the first entry whose document_id
    // is >= document_id: O(log gap) accesses instead of the former linear
    // scan's O(gap), which pays off when an AND operand skips far ahead.
    // A separate skip-list structure is unnecessary because
    // document_id(index) is O(1) random access.
    if (cursor < size && postings.document_id(cursor) < document_id) {
      size_t step = 1;
      auto low = cursor + 1; // document_id(cursor) is known to be < target
      auto high = cursor + step;
      while (high < size && postings.document_id(high) < document_id) {
        low = high + 1;
        step *= 2;
        high = cursor + step;
      }
      if (high > size) {
        high = size;
      }
      // Binary search within [low, high) for the first match; `high` itself
      // is either a known match or `size`.
      while (low < high) {
        auto mid = low + (high - low) / 2;
        if (postings.document_id(mid) < document_id) {
          low = mid + 1;
        } else {
          high = mid;
        }
      }
      cursor = low;
    }

    if (cursor == size) {
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

static void
increment_cursors(std::vector<std::shared_ptr<IPostings>> &positings_list,
                  std::vector<size_t> &cursors,
                  const std::vector<size_t> &slots) {
  for (int i = slots.size() - 1; i >= 0; i--) {
    auto slot = slots[i];
    cursors[slot]++;
    if (cursors[slot] == positings_list[slot]->size()) {
      cursors.erase(cursors.begin() + slot);
      positings_list.erase(positings_list.begin() + slot);
    }
  }
}

static size_t
shortest_slot(const std::vector<std::shared_ptr<IPostings>> &positings_list,
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

static bool
is_adjacent(const std::vector<std::shared_ptr<IPostings>> &positings_list,
            const std::vector<size_t> &cursors, size_t target_slot,
            size_t term_pos) {
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

  if (positings_list.empty()) {
    return result;
  }

  // An empty postings list never intersects with others.
  for (const auto &postings : positings_list) {
    if (postings->size() == 0) {
      return result;
    }
  }

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

static std::shared_ptr<IPostings>
union_postings(std::vector<std::shared_ptr<IPostings>> &&positings_list) {
  positings_list.erase(
      std::remove_if(positings_list.begin(), positings_list.end(),
                     [](const auto &postings) { return postings->size() == 0; }),
      positings_list.end());

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

static std::shared_ptr<IPostings>
perform_term_operation(const IInvertedIndex &inverted_index,
                       const Expression &expr) {
  return std::make_shared<TermSearchResult>(inverted_index, expr.term_str);
}

static std::shared_ptr<IPostings>
perform_and_operation(const IInvertedIndex &inverted_index,
                      const Expression &expr,
                      const IScopeIndex *scope_index) {
  std::vector<Expression> positive_nodes;
  std::vector<Expression> negative_nodes;
  for (const auto &node : expr.nodes) {
    if (node.operation == Operation::Not) {
      negative_nodes.push_back(node.nodes[0]);
    } else {
      positive_nodes.push_back(node);
    }
  }

  auto negative_postings_list =
      positings_list(inverted_index, negative_nodes, scope_index);
  std::vector<size_t> negative_cursors(negative_postings_list.size(), 0);

  return intersect_postings(
      positings_list(inverted_index, positive_nodes, scope_index),
      [&](const auto &positings_list,
          const auto &cursors) -> std::shared_ptr<Position> {
        auto document_id = positings_list[0]->document_id(cursors[0]);

        // Exclude documents that appear in any negative postings. Both sides
        // are iterated in ascending document id order.
        for (size_t slot = 0; slot < negative_postings_list.size(); slot++) {
          const auto &p = negative_postings_list[slot];
          auto &cursor = negative_cursors[slot];
          while (cursor < p->size() && p->document_id(cursor) < document_id) {
            cursor++;
          }
          if (cursor < p->size() && p->document_id(cursor) == document_id) {
            return nullptr;
          }
        }

        std::vector<size_t> slots(positings_list.size(), 0);
        std::iota(slots.begin(), slots.end(), 0);

        std::vector<size_t> term_positions;
        std::vector<size_t> term_lengths;
        merge_term_positions(positings_list, cursors, slots, term_positions,
                             term_lengths);

        return std::make_shared<Position>(document_id,
                                          std::move(term_positions),
                                          std::move(term_lengths));
      });
}

static std::shared_ptr<IPostings>
perform_adjacent_operation(const IInvertedIndex &inverted_index,
                           const Expression &expr,
                           const IScopeIndex *scope_index) {
  return intersect_postings(
      positings_list(inverted_index, expr.nodes, scope_index),
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

static std::shared_ptr<IPostings>
perform_or_operation(const IInvertedIndex &inverted_index,
                     const Expression &expr,
                     const IScopeIndex *scope_index) {
  return union_postings(positings_list(inverted_index, expr.nodes, scope_index));
}

// A Prefix node is answered by expanding it against the index's dictionary
// and unioning the matching terms, i.e. `foo*` behaves exactly like an Or
// over every term starting with `foo`. The expansion happens at search time
// rather than in parse_query because the parser has no index to consult, and
// a parsed Expression is meant to stay reusable across indexes.
static std::shared_ptr<IPostings>
perform_prefix_operation(const IInvertedIndex &inverted_index,
                         const Expression &expr,
                         const IScopeIndex *scope_index) {
  return perform_search_operation(
      inverted_index, expand_prefixes(inverted_index, expr), scope_index);
}

// A Wildcard node is answered the same way a Prefix node is: expand against
// the dictionary and union the matches. Kept as its own operation (rather
// than folding into Prefix) because the two backends answer them with
// different mechanisms -- literal-prefix descent vs. an automaton walk -- and
// mixing that behind one enumerate_terms_with_prefix call would lose the
// cheaper path for the common trailing-`*` case.
static std::shared_ptr<IPostings>
perform_wildcard_operation(const IInvertedIndex &inverted_index,
                           const Expression &expr,
                           const IScopeIndex *scope_index) {
  return perform_search_operation(
      inverted_index, expand_wildcards(inverted_index, expr), scope_index);
}

// And likewise for Fuzzy, which differs from the two above only in which
// dictionary enumeration it expands through.
static std::shared_ptr<IPostings>
perform_fuzzy_operation(const IInvertedIndex &inverted_index,
                        const Expression &expr,
                        const IScopeIndex *scope_index) {
  return perform_search_operation(
      inverted_index, expand_fuzzy(inverted_index, expr), scope_index);
}

static std::shared_ptr<IPostings>
perform_near_operation(const IInvertedIndex &inverted_index,
                       const Expression &expr,
                       const IScopeIndex *scope_index) {
  return intersect_postings(
      positings_list(inverted_index, expr.nodes, scope_index),
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

// Like perform_near_operation, but keeps only the combinations of hits (one
// per node) that fall in the same structural unit (e.g. paragraph), per
// scope_index->scope_id(), instead of within a fixed position distance. If
// scope_index is null, or a candidate document has no scope data registered
// for expr.scope_name, it contributes no matches -- the same "no match"
// treatment as an empty And/Or operand, not an error.
static std::shared_ptr<IPostings>
perform_same_scope_operation(const IInvertedIndex &inverted_index,
                             const Expression &expr,
                             const IScopeIndex *scope_index) {
  if (!scope_index) {
    return std::make_shared<SearchResult>();
  }

  return intersect_postings(
      positings_list(inverted_index, expr.nodes, scope_index),
      [&](const auto &positings_list,
          const auto &cursors) -> std::shared_ptr<Position> {
        auto document_id = positings_list[0]->document_id(cursors[0]);
        if (!scope_index->has_scope(expr.scope_name, document_id)) {
          return nullptr;
        }

        std::vector<size_t> term_positions;
        std::vector<size_t> term_lengths;
        std::vector<size_t> search_hit_cursors(positings_list.size(), 0);

        auto done = false;
        while (!done) {
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

          std::optional<size_t> reference_scope_id;
          auto same_scope = true;
          for (const auto &[term_pos, item] : slots_by_term_pos) {
            auto sid =
                scope_index->scope_id(expr.scope_name, document_id, term_pos);
            if (!reference_scope_id) {
              reference_scope_id = sid;
            } else if (*reference_scope_id != sid) {
              same_scope = false;
              break;
            }
          }

          if (same_scope) {
            for (const auto &[term_pos, item] : slots_by_term_pos) {
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
            // Skip search hit cursor for the smallest slot, same tie-break as
            // perform_near_operation.
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
          return std::make_shared<Position>(document_id,
                                            std::move(term_positions),
                                            std::move(term_lengths));
        }
      });
}

//-----------------------------------------------------------------------------

static std::shared_ptr<IPostings>
perform_search_operation(const IInvertedIndex &inverted_index,
                         const Expression &expr,
                         const IScopeIndex *scope_index) {
  switch (expr.operation) {
  case Operation::Term:
    return perform_term_operation(inverted_index, expr);
  case Operation::And:
    return perform_and_operation(inverted_index, expr, scope_index);
  case Operation::Adjacent:
    return perform_adjacent_operation(inverted_index, expr, scope_index);
  case Operation::Or:
    return perform_or_operation(inverted_index, expr, scope_index);
  case Operation::Near:
    return perform_near_operation(inverted_index, expr, scope_index);
  case Operation::SameScope:
    return perform_same_scope_operation(inverted_index, expr, scope_index);
  case Operation::Prefix:
    return perform_prefix_operation(inverted_index, expr, scope_index);
  case Operation::Wildcard:
    return perform_wildcard_operation(inverted_index, expr, scope_index);
  case Operation::Fuzzy:
    return perform_fuzzy_operation(inverted_index, expr, scope_index);
  default:
    return nullptr;
  }
}

Expression expand_prefixes(const IInvertedIndex &inverted_index,
                           const Expression &expr) {
  if (expr.operation == Operation::Prefix) {
    std::vector<Expression> nodes;
    inverted_index.enumerate_terms_with_prefix(
        expr.term_str, [&](const auto &str) {
          nodes.push_back(Expression{Operation::Term, str});
        });

    // enumerate_terms_with_prefix leaves the order unspecified, and the
    // expanded Expression is handed back to the caller, so sort to keep it
    // reproducible for one prefix against one index.
    std::sort(nodes.begin(), nodes.end(), [](const auto &a, const auto &b) {
      return a.term_str < b.term_str;
    });

    return Expression{Operation::Or, std::u32string(), 0, std::move(nodes)};
  }

  auto expanded = expr;
  for (auto &node : expanded.nodes) {
    node = expand_prefixes(inverted_index, node);
  }
  return expanded;
}

Expression expand_wildcards(const IInvertedIndex &inverted_index,
                            const Expression &expr) {
  if (expr.operation == Operation::Wildcard) {
    std::vector<Expression> nodes;
    inverted_index.enumerate_terms_with_wildcard(
        expr.term_str, [&](const auto &str) {
          nodes.push_back(Expression{Operation::Term, str});
        });

    // enumerate_terms_with_wildcard leaves the order unspecified, same
    // reproducibility rationale as expand_prefixes.
    std::sort(nodes.begin(), nodes.end(), [](const auto &a, const auto &b) {
      return a.term_str < b.term_str;
    });

    return Expression{Operation::Or, std::u32string(), 0, std::move(nodes)};
  }

  auto expanded = expr;
  for (auto &node : expanded.nodes) {
    node = expand_wildcards(inverted_index, node);
  }
  return expanded;
}

Expression expand_fuzzy(const IInvertedIndex &inverted_index,
                        const Expression &expr) {
  if (expr.operation == Operation::Fuzzy) {
    std::vector<Expression> nodes;
    inverted_index.enumerate_terms_with_edit_distance(
        expr.term_str, expr.near_operation_distance, [&](const auto &str) {
          nodes.push_back(Expression{Operation::Term, str});
        });

    // enumerate_terms_with_edit_distance leaves the order unspecified, same
    // reproducibility rationale as expand_prefixes.
    std::sort(nodes.begin(), nodes.end(), [](const auto &a, const auto &b) {
      return a.term_str < b.term_str;
    });

    return Expression{Operation::Or, std::u32string(), 0, std::move(nodes)};
  }

  auto expanded = expr;
  for (auto &node : expanded.nodes) {
    node = expand_fuzzy(inverted_index, node);
  }
  return expanded;
}

std::shared_ptr<IPostings> perform_search(const IInvertedIndex &inverted_index,
                                          const Expression &expr,
                                          const IScopeIndex *scope_index) {
  auto result = perform_search_operation(inverted_index, expr, scope_index);
  // Exclude logically-deleted documents from the final result. Skipped
  // entirely when the index has no tombstones, so the common path is free.
  if (result && inverted_index.has_removed_documents()) {
    return std::make_shared<FilteredPostings>(inverted_index,
                                              std::move(result));
  }
  return result;
}

template <typename T>
void enumerate_terms(const IInvertedIndex &invidx, const Expression &expr,
                     T fn) {
  if (expr.operation == Operation::Term) {
    fn(expr.term_str);
  } else if (expr.operation == Operation::Prefix) {
    // Score a prefix node as the Or it expands to, so that `foo*` and a
    // hand-written Or over the same terms score identically.
    invidx.enumerate_terms_with_prefix(expr.term_str, fn);
  } else if (expr.operation == Operation::Wildcard) {
    // Same rationale as Operation::Prefix above.
    invidx.enumerate_terms_with_wildcard(expr.term_str, fn);
  } else if (expr.operation == Operation::Fuzzy) {
    // Same again; every term within the distance scores as an equal Or
    // branch, with no boost for being a closer match.
    invidx.enumerate_terms_with_edit_distance(
        expr.term_str, expr.near_operation_distance, fn);
  } else if (expr.operation == Operation::Not) {
    // Excluded terms do not contribute to scores.
  } else {
    for (const auto &node : expr.nodes) {
      enumerate_terms(invidx, node, fn);
    }
  }
}

size_t term_count_score(const IInvertedIndex &invidx, const Expression &expr,
                        const IPostings &postings, size_t index) {
  auto document_id = postings.document_id(index);
  size_t score = 0;
  enumerate_terms(invidx, expr, [&](const auto &term) {
    score += invidx.term_count(term, document_id);
  });
  return score;
}

double tf_idf_score(const IInvertedIndex &invidx, const Expression &expr,
                    const IPostings &postings, size_t index) {
  auto document_id = postings.document_id(index);
  auto N = static_cast<double>(invidx.document_count());
  double score = 0.0;
  enumerate_terms(invidx, expr, [&](const auto &term) {
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
  enumerate_terms(invidx, expr, [&](const auto &term) {
    auto n = static_cast<double>(invidx.df(term));
    auto idf = std::log2((N - n + 0.5) / (n + 0.5));
    auto tf = invidx.tf(term, document_id);

    score +=
        idf * ((tf * (k1 + 1.0)) / (tf + k1 * (1.0 - b + b * (dl / avgdl))));
  });
  return score;
}

//-----------------------------------------------------------------------------

BM25Scorer::BM25Scorer(const IInvertedIndex &invidx, const Expression &expr,
                       double k1, double b)
    : invidx_(invidx), N_(static_cast<double>(invidx.document_count())),
      avgdl_(invidx.average_document_term_count()), k1_(k1), b_(b) {
  // The one dictionary walk. enumerate_terms expands Prefix/Wildcard/Fuzzy
  // nodes, which is exactly the work bm25_score repeats for every hit.
  enumerate_terms(invidx, expr, [&](const auto &term) {
    auto postings = invidx.postings(term);
    auto n = static_cast<double>(postings->size());
    terms_.push_back(
        TermState{std::move(postings), std::log2((N_ - n + 0.5) / (n + 0.5))});
  });
}

double BM25Scorer::operator()(const IPostings &postings, size_t index) const {
  auto document_id = postings.document_id(index);
  auto dl = static_cast<double>(invidx_.document_term_count(document_id));
  auto norm = k1_ * (1.0 - b_ + b_ * (dl / avgdl_));

  double score = 0.0;
  for (const auto &term : terms_) {
    // A term the document does not carry contributes with tf == 0 rather
    // than being skipped, so that a degenerate index (avgdl == 0, making
    // norm NaN) produces the same value bm25_score would.
    double tf = 0.0;
    auto i = find_postings_index_for_document_id_(*term.postings, document_id);
    if (i < term.postings->size()) {
      tf = static_cast<double>(term.postings->search_hit_count(i)) / dl;
    }
    score += term.idf * ((tf * (k1_ + 1.0)) / (tf + norm));
  }
  return score;
}

} // namespace searchlib
