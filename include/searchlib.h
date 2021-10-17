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

  virtual size_t document_term_count(size_t document_id) const = 0;
  virtual double average_document_term_count() const = 0;

  virtual bool term_exists(const std::u32string &str) const = 0;
  virtual size_t term_count(const std::u32string &str) const = 0;
  virtual size_t term_count(const std::u32string &str,
                            size_t document_id) const = 0;

  virtual size_t df(const std::u32string &str) const = 0;
  virtual double tf(const std::u32string &str, size_t document_id) const = 0;

  virtual const IPostings &postings(const std::u32string &str) const = 0;
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

enum class Operation { Term, And, Adjacent, Or, Near };

struct Expression {
  Operation operation;
  std::u32string term_str;
  size_t near_operation_distance;
  std::vector<Expression> nodes;
};

std::optional<Expression> parse_query(const IInvertedIndex &invidx,
                                      Normalizer normalizer,
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
// Indexers
//-----------------------------------------------------------------------------

template <typename T> class IIndexer {
public:
  virtual ~IIndexer(){};
  virtual void index_document(size_t document_id, Tokenizer<T> tokenizer) = 0;
};

template <typename T>
inline std::shared_ptr<IInvertedIndexWithTextRange<T>>
make_in_memory_index(Normalizer normalizer,
                     std::function<void(IIndexer<T> &indexer)> callback);

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

  const IPostings &postings(const std::u32string &str) const override;

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

  const IPostings &postings(const std::u32string &str) const override {
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
  InMemoryIndexer(InMemoryInvertedIndex<T> &invidx, Normalizer normalizer)
      : invidx_(invidx), normalizer_(normalizer) {}

  void index_document(size_t document_id, Tokenizer<T> tokenizer) override {
    size_t term_count = 0;
    tokenizer(normalizer_, [&](const auto &str, auto term_pos,
                               auto text_range) {
      if (invidx_.base_.term_dictionary_.find(str) ==
          invidx_.base_.term_dictionary_.end()) {
        invidx_.base_.term_dictionary_[str] = {str, 0};
      }

      auto &term = invidx_.base_.term_dictionary_.at(str);
      term.term_count++;
      term.postings.add_term_position(document_id, term_pos);

      invidx_.text_range_list_[document_id].push_back(std::move(text_range));

      term_count++;
    });

    invidx_.base_.documents_[document_id] = {term_count};
  }

private:
  InMemoryInvertedIndex<T> &invidx_;
  Normalizer normalizer_;
};

template <typename T>
inline std::shared_ptr<IInvertedIndexWithTextRange<T>> make_in_memory_index(
    Normalizer normalizer,
    std::function<void(IIndexer<T> &indexer)> callback) {
  auto invidx = new InMemoryInvertedIndex<T>();
  InMemoryIndexer<T> indexer(*invidx, normalizer);
  callback(indexer);
  return std::shared_ptr<IInvertedIndexWithTextRange<T>>(invidx);
}

} // namespace searchlib
