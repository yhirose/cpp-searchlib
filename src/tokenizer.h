//
//  tokenizer.h
//
//  Copyright (c) 2021 Yuji Hirose. All rights reserved.
//  MIT License
//

#pragma once

#include <string>
#include <string_view>

#include "unicodelib/unicodelib.h"
#include "unicodelib/unicodelib_encodings.h"
#include "searchlib.h"

namespace searchlib {
namespace detail {

// The one definition of "what a raw term is" in this library: a maximal run of
// codepoints for which unicode::is_letter holds. UTF8PlainTextTokenizer,
// utf8_plain_text_splitter() and the segmenting splitter all go through this,
// so there is no second place where the rule could drift.
//
// Calls callback(str, range) once per run with the run's decoded codepoints
// and its byte offsets into `text`. Runs are emitted in increasing order and
// never overlap.
//
// A template taking the callback by deduced type, rather than a plain function
// taking a std::function, because this is the indexing hot path and the
// indirection dominates it. Tokenizing test/t_kjv.tsv (31103 documents) with
// UTF8PlainTextTokenizer and no normalizer, clang -O2 -DNDEBUG, best of 5,
// three interleaved rounds, identical checksums both ways: 22.4-22.6 ms as
// written, 34.0-34.3 ms with this same body behind a `const std::function&`
// parameter instead. So the rule stays defined once, and each caller's lambda
// still inlines into the loop rather than becoming an indirect call per term.
// Taken by forwarding reference rather than by value so that a caller handing
// over a std::function (utf8_plain_text_splitter's `emit`, and parse_query's
// per-token one) does not pay a copy of it -- that copy heap-allocates,
// because the emitters here capture more than the inline buffer holds.
template <typename Callback>
void for_each_letter_run(std::string_view text, Callback &&callback) {
  size_t pos = 0;
  // Hoisted out of the loop so that each run reuses the previous run's
  // capacity: libc++'s u32string holds 5 codepoints inline, so a fresh one per
  // run means a malloc/free for every run longer than that, and this corpus
  // has hundreds of thousands of them. It never outlives the callback call.
  std::u32string str;
  while (pos < text.size()) {
    // Skip
    while (pos < text.size()) {
      char32_t cp;
      auto len =
          unicode::utf8::decode_codepoint(&text[pos], text.size() - pos, cp);
      // decode_codepoint returns 0 without writing cp for anything that is not
      // a well-formed UTF-8 sequence: a truncated one, a bad continuation
      // byte, an overlong encoding, a surrogate, or a value past U+10FFFF.
      // Both halves of that matter: reading cp would be an uninitialized read,
      // and `pos += 0` would spin forever. Since text reaches here straight
      // from a caller's document -- and, via parse_query, from an end-user's
      // query string -- that hang is reachable from untrusted input. Step over
      // the byte instead; it cannot be part of a term either way.
      //
      // Checking this is also what keeps cp inside the range the property
      // tables cover. An earlier vendored unicodelib decoded F7 BF BF BF to
      // U+1FFFFF and is_letter() then read past the end of its table; see
      // TokenizerTest.IllFormedUtf8IsSkipped.
      if (len == 0) {
        pos++;
        continue;
      }
      if (unicode::is_letter(cp)) {
        break;
      }
      pos += len;
    }

    // Term
    auto beg = pos;
    str.clear();

    while (pos < text.size()) {
      char32_t cp;
      auto len =
          unicode::utf8::decode_codepoint(&text[pos], text.size() - pos, cp);
      // Same undecodable case, ending the term rather than skipping: the loop
      // above then steps over the byte, so progress is guaranteed either way.
      if (len == 0 || !unicode::is_letter(cp)) {
        break;
      }
      str += cp;
      pos += len;
    }

    if (!str.empty()) {
      callback(str, TextRange{beg, pos - beg});
    }
  }
}

// Whether `str` contains a Han, Hiragana or Katakana codepoint -- the gate a
// segmenting splitter uses to decide whether a run is worth handing to a
// Japanese segmentation model. Runs without one are left as they are, because
// a model trained on Japanese mangles them, and because that is what makes a
// segmenting splitter's output on non-CJK text identical to
// utf8_plain_text_splitter()'s.
//
// Measured on test/models/ja-ud-gsd.mod, feeding it one letter run at a time
// (which is all this gate ever sees -- a run never contains a space):
// "jumps" comes back as ju/mps, "created" as create/d, "known" as know/n and
// "iPhone" as i/Phone, while most other English words survive intact. So the
// damage is sporadic rather than total, which is worse than it sounds: it is
// invisible until some particular word stops being findable.
inline bool is_cjk_run(const std::u32string &str) {
  for (auto cp : str) {
    // No Han, Hiragana or Katakana codepoint exists below U+2E80 (verified by
    // enumerating U+0000..U+10FFFF against the vendored unicodelib table), so
    // Latin text settles this without touching the script tables at all.
    if (cp < 0x2E80) {
      continue;
    }
    auto sc = unicode::script(cp);
    if (sc == unicode::Script::Han || sc == unicode::Script::Hiragana ||
        sc == unicode::Script::Katakana) {
      return true;
    }
  }
  return false;
}

} // namespace detail
} // namespace searchlib
