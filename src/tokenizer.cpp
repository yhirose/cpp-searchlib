//
//  plain_utf8_tokenizer.cpp
//
//  Copyright (c) 2021 Yuji Hirose. All rights reserved.
//  MIT License
//

#include "lib/unicodelib.h"
#include "lib/unicodelib_encodings.h"
#include "searchlib.h"
#include "utils.h"

using namespace unicode;

namespace searchlib {

ITokenizer::~ITokenizer() = default;

//-----------------------------------------------------------------------------

UTF8PlainTextTokenizer::UTF8PlainTextTokenizer(
    std::string_view sv, Normalizer normalizer,
    std::vector<TextRange> &text_ranges)
    : sv_(sv), normalizer_(normalizer), text_ranges_(text_ranges) {}

void UTF8PlainTextTokenizer::tokenize(TokenizeCallback callback) {
  size_t pos = 0;
  size_t term_pos = 0;
  while (pos < sv_.size()) {
    // Skip
    while (pos < sv_.size()) {
      char32_t cp;
      auto len = utf8::decode_codepoint(&sv_[pos], sv_.size() - pos, cp);
      if (is_letter(cp)) {
        break;
      }
      pos += len;
    }

    // Term
    auto beg = pos;
    std::u32string str;

    while (pos < sv_.size()) {
      char32_t cp;
      auto len = utf8::decode_codepoint(&sv_[pos], sv_.size() - pos, cp);
      if (!is_letter(cp)) {
        break;
      }
      str += cp;
      pos += len;
    }

    if (!str.empty()) {
      callback((normalizer_ ? normalizer_(str) : str), term_pos);
      text_ranges_.push_back({beg, pos - beg});
      term_pos++;
    }
  }
}

}  // namespace searchlib
