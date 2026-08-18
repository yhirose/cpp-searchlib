//
//  searchlib_segment.h
//
//  Copyright (c) 2021 Yuji Hirose. All rights reserved.
//  MIT License
//

#pragma once

#include <string>

#include "searchlib.h"

namespace searchlib {

// A TextSplitter that segments Japanese, backed by the vendored cpp-segmentlib
// (third_party/segmentlib, see its README). Opt-in: this header is separate from
// searchlib.h because using it means shipping a model file, and because the
// single translation unit that implements it is the only one allowed to see
// segmentlib's own vendored fstlib.
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
TextSplitter load_segmenting_splitter(const std::string &model_path);

} // namespace searchlib
