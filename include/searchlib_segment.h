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
// It is the default splitter with a Segmenter plugged in for the scalars
// detail::is_japanese accepts: everything else is cut exactly as
// utf8_plain_text_splitter() cuts it, and only a run of Japanese script reaches
// the model. That gate is what keeps this splitter's output on English
// identical to the default's -- deliberately, because a model trained on
// Japanese otherwise shreds English ("iPhone" -> i/Phone, "jumps" -> ju/mps).
//
// Pass the same splitter to SplitterTokenizer (to index) and to parse_query
// (to search); using it on only one side gives an index where 東京 can be
// found but 東京タワー cannot. Note that switching an existing index to it
// requires a full re-index: the term boundaries, and therefore
// document_term_count and every BM25 score, change.
namespace detail {

// A scalar Japanese is written with: the three scripts, plus the Common and
// Inherited letters and marks that occur inside their words (ー, 々, the
// combining voicing marks). Punctuation, spaces and digits are not, so they
// end a run. One predicate decides both where a span starts and how far it
// extends.
inline bool is_japanese(char32_t cp) {
  switch (unicode::script(cp)) {
  case unicode::Script::Han:
  case unicode::Script::Hiragana:
  case unicode::Script::Katakana:
    return true;
  case unicode::Script::Common:
  case unicode::Script::Inherited:
    return unicode::is_letter(cp) || unicode::is_mark(cp);
  default:
    return false;
  }
}

} // namespace detail

inline TextSplitter load_segmenting_splitter(const std::string &model_path) {
  auto loaded = segmentlib::Segmenter::load(model_path);
  if (!loaded) {
    throw std::runtime_error("searchlib: cannot load segmentation model: " +
                             model_path + ": " +
                             std::string(loaded.error().message));
  }

  // shared_ptr rather than a captured value because a Segmenter is a
  // std::function and gets copied freely; the model is a few megabytes and
  // immutable, so every copy shares this one.
  auto model =
      std::make_shared<const segmentlib::Segmenter>(std::move(*loaded));

  Segmenter segmenter = [model](std::string_view text, size_t offset,
                                const SplitEmit &emit) -> size_t {
    // The run of Japanese scalars from `offset`.
    auto run_end = offset;
    char32_t last = 0; // the run's last scalar, for the boundary test below
    while (run_end < text.size()) {
      char32_t cp;
      auto len = unicode::utf8::decode_codepoint(&text[run_end],
                                                 text.size() - run_end, cp);
      if (len == 0 || !detail::is_japanese(cp)) {
        break;
      }
      run_end += len;
      last = cp;
    }

    // On to the next extended grapheme cluster boundary. The run ends at a
    // scalar that is not Japanese, and one that extends the cluster before it
    // -- a ZWJ, a variation selector -- is not (it is Format or Inherited, not
    // a letter or a mark), so a run can stop inside a cluster. The splitter
    // drops a span that does not end on a boundary, words and all, which would
    // lose 東京 from 東京<ZWJ>タワー. Whether a boundary lies between two
    // scalars depends on those two alone here (a Japanese scalar is never an
    // emoji or a regional indicator, the only things that look further back),
    // so a pair is all the test needs.
    auto end = run_end;
    for (auto prev = last; run_end > offset && end < text.size();) {
      char32_t cp;
      auto len =
          unicode::utf8::decode_codepoint(&text[end], text.size() - end, cp);
      const char32_t pair[] = {prev, cp};
      if (len == 0 || unicode::is_grapheme_boundary(pair, 2, 1)) {
        break;
      }
      end += len;
      prev = cp;
    }

    auto run = text.substr(offset, run_end - offset);
    auto segments = model->tokenize(run);
    if (!segments) {
      // The only failure tokenize() reports is invalid UTF-8, and every byte
      // of `run` was just decoded successfully, so this is not known to be
      // reachable. Emitting the run whole rather than throwing keeps both
      // sides degrading identically, which is the property the design rests
      // on.
      emit(u32(run), TextRange{offset, end - offset});
      return end - offset;
    }

    // The scalars between the run and the boundary belong to the last word's
    // cluster: its range takes them in (it has to end on a boundary), its
    // term does not.
    auto trailing = end - run_end;
    for (const auto &[start, stop] : *segments) {
      if (stop <= start) {
        continue;
      }
      auto word = run.substr(start, stop - start);
      auto length = stop - start + (stop == run.size() ? trailing : 0);
      emit(u32(word), TextRange{offset + start, length});
    }
    return end - offset;
  };

  return utf8_plain_text_splitter(std::move(segmenter), detail::is_japanese);
}

} // namespace searchlib
