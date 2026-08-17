#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <vector>

#include "segmentlib/kytea/char_table.h"
#include "segmentlib/kytea/model.h"
#include "segmentlib/support/attributes.h"
#include "segmentlib/support/span.h"

namespace segmentlib::kytea {

namespace detail {

using Score = std::int32_t;

// Adds n-gram weights (char or type dictionary) to the boundary scores.
// Each match at end position `p` carries a vector of `window*2` weights, one
// per boundary in the window centred on the match, clipped to the sentence.
// (The accumulation itself is negligible next to the automaton traversal —
// profiling shows match-with-add costs the same as match-only — so this stays
// the simple clipped form rather than a padded/SIMD variant.)
// Mirrors FeatureLookup::addNgramScores.
SEGMENTLIB_FLATTEN inline void add_ngram_scores(const Automaton<FeatVec>& dict,
                                                Span<const CharId> str, int window,
                                                std::vector<Score>& scores) {
    if (dict.empty()) {
        return;
    }
    const auto n = static_cast<long>(scores.size());
    dict.match(str, [&](std::size_t end_pos, const FeatVec& vec) {
        const long base = static_cast<long>(end_pos) - window;
        const long start = std::max(0L, -base);
        const long end = std::min<long>(static_cast<long>(window) * 2, n - base);
        for (long j = start; j < end; ++j) {
            scores[static_cast<std::size_t>(base + j)] += vec[static_cast<std::size_t>(j)];
        }
    });
}

// The deduplication scratch for add_dictionary_scores below. thread_local
// keeps the scorer re-entrant; it is reached once per call and bound to a
// reference, rather than named directly inside the per-match callback, because
// resolving a thread_local from there costs an indirect call on every match.
struct DictScratch {
    std::vector<char> seen;  // invariant: all-zero between calls
    std::vector<std::size_t> touched;
};

inline DictScratch& dict_scratch() {
    thread_local DictScratch s;
    return s;
}

// Adds dictionary-word (L/I/R) weights. For every dictionary match, the
// left/inside/right boundaries of the matched span accumulate the corresponding
// weight once each — KyTea's D features are binary presence flags, not counts —
// so a scratch "seen" array deduplicates. Adding on first mark avoids building
// and scanning a full per-position bitmap every sentence. The scratch is left
// all-zero between calls.
// Mirrors FeatureLookup::addDictionaryScores.
SEGMENTLIB_FLATTEN inline void add_dictionary_scores(const Model& model,
                                                     Span<const CharId> chars,
                                                     std::vector<Score>& scores) {
    const FeatVec& dv = model.dict_vector();
    const int num_dicts = model.num_dicts();
    const int max = model.config().dict_n;
    const int len = static_cast<int>(scores.size());
    if (dv.empty() || num_dicts == 0 || len == 0) {
        return;
    }
    const int stride = 3 * max;         // per-position (L,I,R) x label lengths
    const int dict_len = len * stride;  // per sub-dictionary

    DictScratch& ds = dict_scratch();
    std::vector<char>& seen = ds.seen;
    std::vector<std::size_t>& touched = ds.touched;
    const std::size_t need = static_cast<std::size_t>(num_dicts) * dict_len;
    if (seen.size() < need) {
        seen.resize(need, 0);
    }
    touched.clear();

    const auto mark = [&](std::size_t slot, int di, int pos, int col) {
        if (seen[slot] == 0) {
            seen[slot] = 1;
            touched.push_back(slot);
            scores[static_cast<std::size_t>(pos)] +=
                dv[static_cast<std::size_t>(di) * stride + col];
        }
    };

    model.word_dict().match(chars, [&](std::size_t end_pos, const WordEntry& e) {
        if (e.in_dict == 0) {
            return;
        }
        const int end = static_cast<int>(end_pos);
        const int wlen = static_cast<int>(e.char_length);
        const int lablen = std::min(wlen, max) - 1;
        for (int di = 0; di < num_dicts; ++di) {
            if (((1u << di) & e.in_dict) == 0) {
                continue;
            }
            const std::size_t off = static_cast<std::size_t>(di) * dict_len;
            if (end >= wlen) {  // left boundary of the word
                const int pos = end - wlen;
                mark(off + pos * stride + lablen * 3 + 0, di, pos, lablen * 3 + 0);
            }
            for (int k = end - wlen + 1; k < end; ++k) {  // interior boundaries
                mark(off + k * stride + lablen * 3 + 1, di, k, lablen * 3 + 1);
            }
            if (end != len) {  // right boundary of the word
                mark(off + end * stride + lablen * 3 + 2, di, end, lablen * 3 + 2);
            }
        }
    });

    for (const std::size_t slot : touched) {
        seen[slot] = 0;  // restore the all-zero invariant
    }
}

}  // namespace detail

// Same as score_boundaries(), but fills a caller-owned buffer (reused across
// calls to avoid per-call allocation on the hot path). `out` is resized to the
// number of boundaries (N-1).
inline void score_boundaries_into(const Model& model, const EncodedText& enc,
                                  std::vector<std::int32_t>& out) {
    const std::size_t n = enc.length();
    if (n < 2) {
        out.clear();
        return;
    }
    const std::int32_t bias = model.biases().empty() ? 0 : model.biases()[0];
    out.assign(n - 1, bias);

    detail::add_ngram_scores(model.char_dict(), enc.char_ids, model.config().char_window,
                             out);
    detail::add_ngram_scores(model.type_dict(), enc.type_ids, model.config().type_window,
                             out);
    detail::add_dictionary_scores(model, enc.char_ids, out);
}

// Computes KyTea's per-boundary word-segmentation scores for an encoded
// sentence (the algorithm of `Kytea::calculateWS`). The result has one entry
// per character boundary (length N-1 for N characters; empty if N < 2).
//
// A boundary i sits between character i and i+1. Multiplying by the model's
// multiplier yields KyTea's confidence; a boundary is placed where that
// confidence is > 0. Since the multiplier is positive, the raw score's sign is
// what matters. Scores accumulate three feature families:
//   1. a constant bias,
//   2. character and character-type n-gram weights (Aho-Corasick matches), and
//   3. dictionary (L/I/R) weights from matched dictionary words.
[[nodiscard]] inline std::vector<std::int32_t> score_boundaries(const Model& model,
                                                                const EncodedText& enc) {
    std::vector<std::int32_t> scores;
    score_boundaries_into(model, enc, scores);
    return scores;
}

}  // namespace segmentlib::kytea
