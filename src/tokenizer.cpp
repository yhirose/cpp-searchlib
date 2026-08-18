//
//  tokenizer.cpp
//
//  Copyright (c) 2021 Yuji Hirose. All rights reserved.
//  MIT License
//

#include "tokenizer.h"

#include "searchlib.h"

namespace searchlib {

TextRange text_range(const TextRangeList<TextRange> &text_range_list,
                     const IPostings &positions, size_t index,
                     size_t search_hit_index) {
  auto document_id = positions.document_id(index);
  auto term_pos = positions.term_position(index, search_hit_index);
  auto term_length = positions.term_length(index, search_hit_index);
  if (term_length == 1) {
    return text_range_list.at(document_id)[term_pos];
  } else {
    auto beg = text_range_list.at(document_id)[term_pos];
    auto end = text_range_list.at(document_id)[term_pos + term_length - 1];
    auto length = end.position + end.length - beg.position;
    return TextRange{beg.position, length};
  }
}

//-----------------------------------------------------------------------------

TextSplitter utf8_plain_text_splitter() {
  return [](std::string_view text, const auto &emit) {
    detail::for_each_letter_run(text, emit);
  };
}

//-----------------------------------------------------------------------------

UTF8PlainTextTokenizer::UTF8PlainTextTokenizer(std::string_view text)
    : text_(text) {}

// The body below is SplitterTokenizer's null-splitter branch written out
// again, which is a deliberate few lines of duplication: delegating to
// SplitterTokenizer(nullptr, text_) measures slower on this, the default
// indexing path. Tokenizing test/t_kjv.tsv (31103 documents), clang -O2
// -DNDEBUG, best of 5, three interleaved rounds, identical output both ways:
// 20.0-20.3 ms direct against 21.8-22.0 ms delegated. The cost is per document
// rather than per term -- building the temporary and moving the two
// std::functions into it, plus a call that no longer inlines -- so it does not
// grow with document length, but it is real and every existing caller of this
// library takes this path.
void UTF8PlainTextTokenizer::operator()(
    Normalizer normalizer,
    std::function<void(const std::u32string &str, size_t term_pos,
                       TextRange text_range)>
        callback) {
  size_t term_pos = 0;
  detail::for_each_letter_run(
      text_, [&](const std::u32string &str, TextRange range) {
        // Spelled as an if rather than `callback(normalizer ? normalizer(str)
        // : str, ...)`: one arm of that conditional is a prvalue, so the whole
        // expression is a prvalue and `str` gets copied on every term even
        // when there is no normalizer at all. See SplitterTokenizer below.
        if (normalizer) {
          callback(normalizer(str), term_pos, range);
        } else {
          callback(str, term_pos, range);
        }
        term_pos++;
      });
}

//-----------------------------------------------------------------------------

SplitterTokenizer::SplitterTokenizer(TextSplitter splitter,
                                     std::string_view text)
    : splitter_(std::move(splitter)), text_(text) {}

void SplitterTokenizer::operator()(
    Normalizer normalizer,
    std::function<void(const std::u32string &str, size_t term_pos,
                       TextRange text_range)>
        callback) {
  size_t term_pos = 0;
  auto emit = [&](const std::u32string &str, TextRange range) {
    // Not a ternary, for the reason given in UTF8PlainTextTokenizer above: it
    // would copy every term whether or not a normalizer is configured.
    if (normalizer) {
      callback(normalizer(str), term_pos, range);
    } else {
      callback(str, term_pos, range);
    }
    term_pos++;
  };
  if (splitter_) {
    splitter_(text_, emit);
  } else {
    detail::for_each_letter_run(text_, emit);
  }
}

} // namespace searchlib
