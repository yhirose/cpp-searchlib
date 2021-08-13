//
//  searchlib.h
//
//  Copyright (c) 2021 Yuji Hirose. All rights reserved.
//  MIT License
//

#pragma once

#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace searchlib {

//-----------------------------------------------------------------------------
// Indexer
//-----------------------------------------------------------------------------

using Normalizer = std::function<std::u32string(std::u32string_view)>;

using PositionalList =
    std::map<size_t /*document_id*/, std::vector<size_t /*position*/>>;

struct InvertedIndex {
  std::map<std::u32string /*str*/, size_t /*term_id*/> term_dictionary;
  std::vector<std::u32string /*str*/> terms;
  std::vector<PositionalList> posting_list;

  Normalizer normalizer;
};

class Tokenizer {
 public:
  virtual ~Tokenizer() = 0;

  using TokenizeCallback =
      std::function<void(const std::u32string &str, size_t term_pos)>;

  virtual void tokenize(TokenizeCallback callback) = 0;
};

void indexing(InvertedIndex &index, Tokenizer &tokenizer, size_t document_id);

//-----------------------------------------------------------------------------
// Search
//-----------------------------------------------------------------------------

enum class Operation { Term, And, Adjacent, Or };

struct Expression {
  Operation operation;
  size_t term_id;
  std::vector<Expression> nodes;
};

std::optional<Expression> parse_query(const InvertedIndex &index,
                                      std::string_view sv);

class SearchResult {
 public:
  virtual ~SearchResult() = 0;

  virtual size_t size() const = 0;

  virtual size_t document_id(size_t index) const = 0;
  virtual size_t search_hit_count(size_t index) const = 0;

  virtual size_t term_position(size_t index, size_t search_hit_index) const = 0;
  virtual size_t term_count(size_t index, size_t search_hit_index) const = 0;
  virtual bool has_term_pos(size_t index, size_t term_pos) const = 0;
};

std::shared_ptr<SearchResult> perform_search(const InvertedIndex &index,
                                             const Expression &expr);

//-----------------------------------------------------------------------------
// Text Range
//-----------------------------------------------------------------------------

struct TextRange {
  size_t position;
  size_t length;
};

using TextRangeList = std::map<size_t /*document_id*/, std::vector<TextRange>>;

TextRange text_range(const TextRangeList &text_range_list,
                     const SearchResult &result, size_t index,
                     size_t search_hit_index);

//-----------------------------------------------------------------------------
// Tokenizers
//-----------------------------------------------------------------------------

class UTF8PlainTextTokenizer : public Tokenizer {
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
