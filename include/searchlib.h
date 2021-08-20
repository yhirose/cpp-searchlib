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
// Inverted Index
//-----------------------------------------------------------------------------

struct Term {
  std::u32string str;
  size_t tf;
};

class IPostings {
 public:
  virtual ~IPostings() = 0;
  virtual size_t size() const = 0;
  virtual size_t document_id(size_t index) const = 0;
  virtual size_t search_hit_count(size_t index) const = 0;
  virtual size_t term_position(size_t index, size_t search_hit_index) const = 0;
  virtual size_t term_count(size_t index, size_t search_hit_index) const = 0;
  virtual bool has_term_pos(size_t index, size_t term_pos) const = 0;
};

class IInvertedIndex {
 public:
  virtual ~IInvertedIndex() = 0;

  virtual std::u32string normalize(const std::u32string &str) const = 0;

  virtual size_t document_count() const = 0;

  virtual bool has_term(const std::u32string &str) const = 0;
  virtual size_t term_id(const std::u32string &str) const = 0;

  virtual size_t tf(size_t term_id) const = 0;
  virtual size_t df(size_t term_id) const = 0;

  virtual const IPostings &postings(size_t term_id) const = 0;
};

using Normalizer = std::function<std::u32string(const std::u32string &str)>;

//-----------------------------------------------------------------------------
// Search
//-----------------------------------------------------------------------------

enum class Operation { Term, And, Adjacent, Or, Near };

struct Expression {
  Operation operation;
  size_t term_id;
  size_t near_size;
  std::vector<Expression> nodes;
};

std::optional<Expression> parse_query(const IInvertedIndex &index,
                                      std::string_view sv);

std::shared_ptr<IPostings> perform_search(const IInvertedIndex &index,
                                          const Expression &expr);

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
// OnMemoryIndxer
//-----------------------------------------------------------------------------

class OnMemoryIndxer : public IInvertedIndex {
 public:
  std::u32string normalize(const std::u32string &str) const override;

  size_t document_count() const override;

  bool has_term(const std::u32string &str) const override;
  size_t term_id(const std::u32string &str) const override;

  size_t tf(size_t term_id) const override;
  size_t df(size_t term_id) const override;

  // For indexing
  const IPostings &postings(size_t term_id) const override;

  class ITokenizer {
   public:
    virtual ~ITokenizer() = 0;

    using TokenizeCallback =
        std::function<void(const std::u32string &str, size_t term_pos)>;

    virtual void tokenize(TokenizeCallback callback) = 0;
  };

  void indexing(size_t document_id, ITokenizer &tokenizer);

  Normalizer normalizer;

 private:
  class Postings : public IPostings {
   public:
    size_t size() const override;
    size_t document_id(size_t index) const override;
    size_t search_hit_count(size_t index) const override;
    size_t term_position(size_t index, size_t search_hit_index) const override;
    size_t term_count(size_t index, size_t search_hit_index) const override;
    bool has_term_pos(size_t index, size_t term_pos) const override;

    void add_term_position(size_t document_id, size_t term_pos);

   private:
    using PositionsMap =
        std::map<size_t /*document_id*/, std::vector<size_t /*position*/>>;

    PositionsMap::const_iterator find_positions_map(size_t index) const;
    PositionsMap positions_map_;
  };

  size_t document_count_ = 0;
  std::unordered_map<std::u32string /*str*/, size_t /*term_id*/>
      term_dictionary_;
  std::vector<Term> terms_;
  std::vector<Postings> postings_list_;
};

//-----------------------------------------------------------------------------
// Tokenizers
//-----------------------------------------------------------------------------

class UTF8PlainTextTokenizer : public OnMemoryIndxer::ITokenizer {
 public:
  UTF8PlainTextTokenizer(std::string_view sv, Normalizer normalizer,
                         std::vector<TextRange> &text_ranges);
  void tokenize(TokenizeCallback callback) override;

 private:
  std::string_view sv_;
  Normalizer normalizer_;
  std::vector<TextRange> &text_ranges_;
};

}  // namespace searchlib
