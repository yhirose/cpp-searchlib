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
// Inverted Index Interface
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

  virtual std::u32string normalize(const std::u32string &str) const = 0;
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
                                      std::string_view query);

std::shared_ptr<IPostings> perform_search(const IInvertedIndex &invidx,
                                          const Expression &expr);

size_t term_count_score(const IInvertedIndex &invidx, const Expression &expr,
                        const IPostings &postings, size_t index);

double tf_score(const IInvertedIndex &invidx, const Expression &expr,
                const IPostings &postings, size_t index);

double tf_idf_score(const IInvertedIndex &invidx, const Expression &expr,
                    const IPostings &postings, size_t index);

double bm25_score(const IInvertedIndex &invidx, const Expression &expr,
                  const IPostings &postings, size_t index, double k1 = 1.2,
                  double b = 0.75);

//-----------------------------------------------------------------------------
// Text Range
//-----------------------------------------------------------------------------

struct TextRange {
  size_t position;
  size_t length;
};

using TextRangeList =
    std::unordered_map<size_t /*document_id*/, std::vector<TextRange>>;

TextRange text_range(const TextRangeList &text_range_list,
                     const IPostings &postions, size_t index,
                     size_t search_hit_index);

//-----------------------------------------------------------------------------
// InvertedIndex
//-----------------------------------------------------------------------------

class InvertedIndex : public IInvertedIndex {
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

  std::u32string normalize(const std::u32string &str) const override;

 private:
  friend class Indexer;

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

  std::function<std::u32string(const std::u32string &str)> normalizer_;
  std::unordered_map<size_t /*document_id*/, Document> documents_;
  std::unordered_map<std::u32string /*str*/, Term> term_dictionary_;
};

//-----------------------------------------------------------------------------
// Indexer
//-----------------------------------------------------------------------------

class ITokenizer {
 public:
  virtual ~ITokenizer() = 0;

  virtual void tokenize(
      std::function<std::u32string(const std::u32string &str)> normalizer,
      std::function<void(const std::u32string &str, size_t term_pos)>
          callback) = 0;
};

class Indexer {
 public:
  Indexer() = delete;

  static void set_normalizer(
      InvertedIndex &invidx,
      std::function<std::u32string(const std::u32string &str)> normalizer);

  static void indexing(InvertedIndex &invidx, size_t document_id,
                       ITokenizer &tokenizer);
};

//-----------------------------------------------------------------------------
// Tokenizers
//-----------------------------------------------------------------------------

class UTF8PlainTextTokenizer : public ITokenizer {
 public:
  UTF8PlainTextTokenizer(std::string_view text,
                         std::vector<TextRange> *text_ranges = nullptr);

  void tokenize(
      std::function<std::u32string(const std::u32string &str)> normalizer,
      std::function<void(const std::u32string &str, size_t term_pos)> callback)
      override;

 private:
  std::string_view text_;
  std::vector<TextRange> *text_ranges_;
};

}  // namespace searchlib
