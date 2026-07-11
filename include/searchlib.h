//
//  searchlib.h
//
//  Copyright (c) 2021 Yuji Hirose. All rights reserved.
//  MIT License
//

#pragma once

#include <algorithm>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace searchlib {

//-----------------------------------------------------------------------------
// Interface
//-----------------------------------------------------------------------------

class IPostings {
public:
  virtual ~IPostings() = 0;

  virtual size_t size() const = 0;

  // document_id is scoped to the IInvertedIndex instance that produced this
  // IPostings; it is not a globally unique identifier across indexes. Code
  // that fans out across multiple indexes (e.g. a future federated search
  // layer) must tag results with their originating index rather than
  // assuming document_id alone is enough to disambiguate them.
  virtual size_t document_id(size_t index) const = 0;
  virtual size_t search_hit_count(size_t index) const = 0;

  virtual size_t term_position(size_t index, size_t search_hit_index) const = 0;
  virtual size_t term_length(size_t index, size_t search_hit_index) const = 0;
  virtual bool is_term_position(size_t index, size_t term_pos) const = 0;
};

class IInvertedIndex {
public:
  virtual ~IInvertedIndex() = 0;

  virtual size_t document_count() const = 0;

  // document_id here (and everywhere else in this interface) is local to
  // this IInvertedIndex instance; see the note on IPostings::document_id.
  virtual size_t document_term_count(size_t document_id) const = 0;
  virtual double average_document_term_count() const = 0;

  virtual bool term_exists(const std::u32string &str) const = 0;
  virtual size_t term_count(const std::u32string &str) const = 0;
  virtual size_t term_count(const std::u32string &str,
                            size_t document_id) const = 0;

  virtual size_t df(const std::u32string &str) const = 0;
  virtual double tf(const std::u32string &str, size_t document_id) const = 0;

  virtual std::shared_ptr<const IPostings>
  postings(const std::u32string &str) const = 0;
};

using Normalizer = std::function<std::u32string(const std::u32string &str)>;

template <typename T>
using TextRangeList =
    std::unordered_map<size_t /*document_id*/, std::vector<T>>;

template <typename T>
using Tokenizer =
    std::function<void(Normalizer normalizer,
                       std::function<void(const std::u32string &str,
                                          size_t term_pos, T text_range)>
                           callback)>;

template <typename T> class ITextRange {
public:
  virtual ~ITextRange(){};

  virtual T text_range(const IPostings &positions, size_t index,
                       size_t search_hit_index) const = 0;
};

template <typename T>
class IInvertedIndexWithTextRange : public IInvertedIndex, ITextRange<T> {
public:
  virtual ~IInvertedIndexWithTextRange(){};
};

//-----------------------------------------------------------------------------
// Search
//-----------------------------------------------------------------------------

enum class Operation { Term, And, Adjacent, Or, Near, Not };

struct Expression {
  Operation operation;
  std::u32string term_str;
  size_t near_operation_distance;
  std::vector<Expression> nodes;
};

std::optional<Expression> parse_query(Normalizer normalizer,
                                      std::string_view query);

std::shared_ptr<IPostings> perform_search(const IInvertedIndex &invidx,
                                          const Expression &expr);

size_t term_count_score(const IInvertedIndex &invidx, const Expression &expr,
                        const IPostings &postings, size_t index);

double tf_idf_score(const IInvertedIndex &invidx, const Expression &expr,
                    const IPostings &postings, size_t index);

double bm25_score(const IInvertedIndex &invidx, const Expression &expr,
                  const IPostings &postings, size_t index, double k1 = 1.2,
                  double b = 0.75);

//-----------------------------------------------------------------------------
// Federated Search
//-----------------------------------------------------------------------------

// A mutable counterpart to IInvertedIndex. Kept as a separate interface
// (rather than discovered via dynamic_pointer_cast<IMutableInvertedIndex>)
// so that callers who already know an index is writable at registration
// time can pass that fact along explicitly, with no RTTI involved.
class IMutableInvertedIndex {
public:
  virtual ~IMutableInvertedIndex() = 0;

  // document_id is local to the corresponding IInvertedIndex; see the note
  // on IPostings::document_id.
  virtual void remove_document(size_t document_id) = 0;
};

// One entry in a FederatedIndex. mutable_index is null for read-only
// members (e.g. an installed book) and non-null for members that support
// removal (e.g. a notes index), decided by the caller at registration time.
struct FederationMember {
  std::shared_ptr<IInvertedIndex> index;
  std::shared_ptr<IMutableInvertedIndex> mutable_index;
};

// One hit produced by perform_federated_search. document ids are only
// meaningful together with `index`, since each member has its own local id
// space (see the note on IPostings::document_id).
struct FederatedHit {
  std::shared_ptr<IInvertedIndex> index;
  std::shared_ptr<IPostings> postings;
  size_t index_in_postings;
};

// A dynamic collection of independently-built IInvertedIndex instances that
// can be searched as one. Members are added/removed by shared_ptr identity;
// snapshot() returns a copy of the member list so that a search in
// progress is unaffected by concurrent add()/remove() calls (the
// underlying indexes are kept alive by the shared_ptr in the snapshot).
//
// This class only guards its own member list; it does not make any
// individual IInvertedIndex/IMutableInvertedIndex thread-safe on its own.
class FederatedIndex {
public:
  void add(std::shared_ptr<IInvertedIndex> index,
           std::shared_ptr<IMutableInvertedIndex> mutable_index = nullptr);
  void remove(const std::shared_ptr<IInvertedIndex> &index);

  std::vector<FederationMember> snapshot() const;

private:
  mutable std::mutex mutex_;
  std::vector<FederationMember> members_;
};

// Evaluates `expr` independently against every member of `federation`
// (each member resolves its own And/Or/Near/Not cursors internally, see
// perform_search) and concatenates the results, tagged with the
// originating member. No cross-member score normalization or sorting is
// performed; callers that need a combined ranking must do so themselves.
std::vector<FederatedHit> perform_federated_search(const FederatedIndex &federation,
                                                   const Expression &expr);

//-----------------------------------------------------------------------------
// Indexers
//-----------------------------------------------------------------------------

template <typename T> class IIndexer {
public:
  virtual ~IIndexer(){};

  // document_id is chosen by the caller and is only meaningful within the
  // IInvertedIndex this indexer writes to; see the note on
  // IPostings::document_id.
  virtual void index_document(size_t document_id, Tokenizer<T> tokenizer) = 0;
};

//-----------------------------------------------------------------------------
// Text Ranges
//-----------------------------------------------------------------------------

struct TextRange {
  size_t position;
  size_t length;
};

//-----------------------------------------------------------------------------
// Tokenizers
//-----------------------------------------------------------------------------

class UTF8PlainTextTokenizer {
public:
  explicit UTF8PlainTextTokenizer(std::string_view text);

  void operator()(Normalizer normalizer,
                  std::function<void(const std::u32string &str, size_t term_pos,
                                     TextRange text_range)>
                      callback);

private:
  std::string_view text_;
};

//-----------------------------------------------------------------------------

TextRange text_range(const TextRangeList<TextRange> &text_range_list,
                     const IPostings &positions, size_t index,
                     size_t search_hit_index);

class InMemoryInvertedIndexBase : public IInvertedIndex {
public:
  size_t document_count() const override;

  size_t document_term_count(size_t document_id) const override;
  double average_document_term_count() const override;

  bool term_exists(const std::u32string &str) const override;
  size_t term_count(const std::u32string &str) const override;
  size_t term_count(const std::u32string &str,
                    size_t document_id) const override;

  size_t df(const std::u32string &str) const override;
  double tf(const std::u32string &str, size_t document_id) const override;

  std::shared_ptr<const IPostings>
  postings(const std::u32string &str) const override;

  class Postings : public IPostings {
  public:
    size_t size() const override;

    size_t document_id(size_t index) const override;
    size_t search_hit_count(size_t index) const override;

    size_t term_position(size_t index, size_t search_hit_index) const override;
    size_t term_length(size_t index, size_t search_hit_index) const override;
    bool is_term_position(size_t index, size_t term_pos) const override;

    void add_term_position(size_t document_id, size_t term_pos);

  private:
    using PositionsMap =
        std::map<size_t /*document_id*/, std::vector<size_t /*position*/>>;

    PositionsMap::const_iterator find_positions_map(size_t index) const;
    PositionsMap positions_map_;
  };

  struct Document {
    size_t term_count;
  };

  struct Term {
    std::u32string str;
    size_t term_count;
    Postings postings;
  };

  std::unordered_map<size_t /*document_id*/, Document> documents_;
  std::unordered_map<std::u32string /*str*/, Term> term_dictionary_;
};

template <typename T>
class InMemoryInvertedIndex : public IInvertedIndexWithTextRange<T> {
public:
  size_t document_count() const override { return base_.document_count(); }

  size_t document_term_count(size_t document_id) const override {
    return base_.document_term_count(document_id);
  }

  double average_document_term_count() const override {
    return base_.average_document_term_count();
  }

  bool term_exists(const std::u32string &str) const override {
    return base_.term_exists(str);
  }

  size_t term_count(const std::u32string &str) const override {
    return base_.term_count(str);
  }

  size_t term_count(const std::u32string &str,
                    size_t document_id) const override {
    return base_.term_count(str, document_id);
  }

  size_t df(const std::u32string &str) const override { return base_.df(str); }

  double tf(const std::u32string &str, size_t document_id) const override {
    return base_.tf(str, document_id);
  }

  std::shared_ptr<const IPostings>
  postings(const std::u32string &str) const override {
    return base_.postings(str);
  }

  T text_range(const IPostings &positions, size_t index,
               size_t search_hit_index) const override {
    return searchlib::text_range(text_range_list_, positions, index,
                                 search_hit_index);
  }

private:
  template <typename> friend class InMemoryIndexer;

  InMemoryInvertedIndexBase base_;
  TextRangeList<T> text_range_list_;
};

template <typename T> class InMemoryIndexer : public IIndexer<T> {
public:
  InMemoryIndexer(InMemoryInvertedIndex<T> &index, Normalizer normalizer)
      : index_(index), normalizer_(normalizer) {}

  void index_document(size_t document_id, Tokenizer<T> tokenizer) override {
    size_t term_count = 0;
    tokenizer(normalizer_, [&](const auto &str, auto term_pos,
                               auto text_range) {
      if (index_.base_.term_dictionary_.find(str) ==
          index_.base_.term_dictionary_.end()) {
        index_.base_.term_dictionary_[str] = {str, 0};
      }

      auto &term = index_.base_.term_dictionary_.at(str);
      term.term_count++;
      term.postings.add_term_position(document_id, term_pos);

      index_.text_range_list_[document_id].push_back(std::move(text_range));

      term_count++;
    });

    index_.base_.documents_[document_id] = {term_count};
  }

private:
  InMemoryInvertedIndex<T> &index_;
  Normalizer normalizer_;
};

} // namespace searchlib
