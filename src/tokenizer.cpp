//
//  tokenizer.cpp
//
//  Copyright (c) 2021 Yuji Hirose. All rights reserved.
//  MIT License
//

#include "lib/unicodelib.h"
#include "lib/unicodelib_encodings.h"
#include "searchlib.h"

using namespace unicode;

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

UTF8PlainTextTokenizer::UTF8PlainTextTokenizer(std::string_view text)
    : text_(text) {}

void UTF8PlainTextTokenizer::operator()(
    Normalizer normalizer,
    std::function<void(const std::u32string &str, size_t term_pos,
                       TextRange text_range)>
        callback) {
  size_t pos = 0;
  size_t term_pos = 0;
  while (pos < text_.size()) {
    // Skip
    while (pos < text_.size()) {
      char32_t cp;
      auto len = utf8::decode_codepoint(&text_[pos], text_.size() - pos, cp);
      if (is_letter(cp)) {
        break;
      }
      pos += len;
    }

    // Term
    auto beg = pos;
    std::u32string str;

    while (pos < text_.size()) {
      char32_t cp;
      auto len = utf8::decode_codepoint(&text_[pos], text_.size() - pos, cp);
      if (!is_letter(cp)) {
        break;
      }
      str += cp;
      pos += len;
    }

    if (!str.empty()) {
      callback((normalizer ? normalizer(str) : str), term_pos,
               {beg, pos - beg});
      term_pos++;
    }
  }
}

} // namespace searchlib
