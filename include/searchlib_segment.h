//
//  searchlib_segment.h
//
//  Copyright (c) 2021 Yuji Hirose. All rights reserved.
//  MIT License
//

#pragma once

#include <memory>
#include <stdexcept>
#include <string>

#include <segmentlib/segmenter.h>

#include "searchlib.h"

namespace searchlib {

// A TextSplitter that segments Japanese, backed by the vendored cpp-segmentlib
// (third_party/segmentlib, see its README). Opt-in: this header is separate
// from searchlib.h because using it means compiling segmentlib and shipping a
// model file, neither of which a caller indexing English should have to carry.
// Its include root is `third_party` itself, and as `-isystem`: segmentlib is
// third-party code and is not warning-clean under -Wall -Wextra.
//
// Only the segmentlib types stay hidden, not the model: `model_path` names a
// file the caller supplies, in any format segmentlib::Segmenter::load accepts
// (the MLP format is the small one -- upstream's reference model is 2.1 MB
// against the KyTea model's 122 MB). Throws std::runtime_error if it cannot be
// loaded. The returned splitter shares one immutable model between all its
// copies and is safe to call from several threads.
//
// It refines utf8_plain_text_splitter() rather than replacing it: terms are
// still cut at Unicode letter-run boundaries, and only runs containing Han,
// Hiragana or Katakana are handed to the model, which then adds boundaries
// inside them. So the output on text with no CJK in it is byte-for-byte what
// the default splitter produces -- deliberately, because a model trained on
// Japanese otherwise shreds English ("iPhone" -> i/Phone).
//
// Pass the same splitter to SplitterTokenizer (to index) and to parse_query
// (to search); using it on only one side gives an index where 東京 can be
// found but 東京タワー cannot. Note that switching an existing index to it
// requires a full re-index: the term boundaries, and therefore
// document_term_count and every BM25 score, change.
inline TextSplitter load_segmenting_splitter(const std::string &model_path) {
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
            // The only failure tokenize() reports is invalid UTF-8, and no
            // input is known to reach it: every byte of `run` was consumed by
            // a successful unicode::utf8::decode_codepoint, and the two
            // vendored decoders reject the same six classes (bad lead byte,
            // truncated, bad continuation, overlong, surrogate, past
            // U+10FFFF). Fuzzing 200000 ill-formed byte strings through this
            // loop produced 87805 CJK runs and zero failures. The branch stays
            // because that agreement is between two independently vendored
            // libraries, either of which can move.
            //
            // Fall back to the unsegmented run rather than throwing: the query
            // side runs this same code from inside a PEG semantic action, and
            // both sides degrading identically keeps their term boundaries in
            // agreement, which is the one property the whole design rests on.
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
