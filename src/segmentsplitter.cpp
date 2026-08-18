//
//  segmentsplitter.cpp
//
//  Copyright (c) 2021 Yuji Hirose. All rights reserved.
//  MIT License
//

// The one translation unit that sees cpp-segmentlib. It is compiled as its own
// CMake target so that `-isystem src/lib` reaches nothing else: segmentlib
// carries its own vendored copy of fstlib, at a different revision from this
// project's, and the two both define `namespace fst`, so a file that included
// src/termdict.h alongside these headers would fail with a wall of
// redefinition errors. Keeping the include path scoped to this one file makes
// that collision unreachable rather than merely avoided by convention.
// See src/lib/segmentlib/README.md.

#include <memory>
#include <stdexcept>
#include <string>

#include "searchlib_segment.h"
#include "segmentlib/segmenter.h"
#include "tokenizer.h"
#include "utils.h"

namespace searchlib {

TextSplitter load_segmenting_splitter(const std::string &model_path) {
  auto loaded = segmentlib::Segmenter::load(model_path);
  if (!loaded) {
    throw std::runtime_error("searchlib: cannot load segmentation model: " +
                             model_path + ": " +
                             std::string(loaded.error().message));
  }

  // shared_ptr rather than a captured value because a TextSplitter is a
  // std::function and gets copied freely (parse_query takes one by value); the
  // model is a few megabytes and immutable, so every copy shares this one.
  auto segmenter =
      std::make_shared<const segmentlib::Segmenter>(std::move(*loaded));

  return [segmenter](std::string_view text, const auto &emit) {
    detail::for_each_letter_run(
        text, [&](const std::u32string &str, TextRange range) {
          // Non-CJK runs are emitted exactly as the default splitter would,
          // which is what keeps this splitter's behaviour on English text
          // identical to utf8_plain_text_splitter()'s.
          if (!detail::is_cjk_run(str)) {
            emit(str, range);
            return;
          }

          auto run = text.substr(range.position, range.length);
          auto segments = segmenter->tokenize(run);
          if (!segments) {
            // The only failure the model reports is invalid UTF-8, and it is
            // reachable: unicode::utf8::decode_codepoint does not validate
            // continuation bytes, so a malformed sequence can decode to a
            // letter codepoint and carry its raw bytes into a run (verified
            // with "\xE3\x81\x41", which arrives here as one Hiragana run and
            // is then rejected). Fall back to the unsegmented run rather than
            // throwing: the query side runs this same code from inside a PEG
            // semantic action, and both sides degrading identically keeps
            // their term boundaries in agreement, which is the one property
            // the whole design rests on.
            emit(str, range);
            return;
          }

          for (const auto &[start, end] : *segments) {
            if (end <= start) {
              continue;
            }
            auto word = run.substr(start, end - start);
            emit(u32(word), TextRange{range.position + start, end - start});
          }
        });
  };
}

} // namespace searchlib
