#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <vector>

#include "segmentlib/mlp/dictionary.h"
#include "segmentlib/mlp/kernels.h"
#include "segmentlib/mlp/model.h"
#include "segmentlib/mlp/vocab.h"
#include "segmentlib/support/span.h"

namespace segmentlib::mlp {

// Per-call buffers for the scorer: the Int32 accumulator and the
// dictionary-feature scratch. score_boundaries_into allocates nothing once
// these have grown to the sentence's size, matching the kytea::scorer _into
// convention.
struct Workspace {
    std::vector<std::int32_t> acc;  // H (TablePrecision::Int32)
    DictFeatures dict;
    // Int16 fused path (kernels::fused_score_i16): the per-boundary block
    // pointers (2w window slots then active dict columns) and 2w×H scratch for
    // the window slots whose EGC needs fallback synthesis.
    std::vector<const std::int16_t*> blocks;
    std::vector<std::int16_t> fallback;
};

namespace detail {

inline std::int32_t saturate(std::int64_t sum) noexcept {
    return static_cast<std::int32_t>(
        std::clamp<std::int64_t>(sum, std::numeric_limits<std::int32_t>::min(),
                                 std::numeric_limits<std::int32_t>::max()));
}

// Rows of the EGC covered by window slot j of boundary i (EGC i - w + 1 + j);
// slots past either text end resolve to the PAD pseudo-EGC, itself a normal
// single-row table entry (I.2). Shared by the Int32 and Int16 scoring loops so
// this correctness-critical signed slot-index arithmetic lives in one place.
inline Span<const std::uint32_t> slot_rows(const EncodedEgc& enc, std::size_t m,
                                           std::size_t i, std::size_t j,
                                           std::uint32_t w) {
    static constexpr std::uint32_t pad[] = {kPadRow};
    const auto egc = static_cast<std::int64_t>(i) - static_cast<std::int64_t>(w) +
                     1 + static_cast<std::int64_t>(j);
    return (egc >= 0 && egc < static_cast<std::int64_t>(m))
               ? enc.egc_rows(static_cast<std::size_t>(egc))
               : Span<const std::uint32_t>(pad, 1);
}

// The Int32 (exact) path: the per-boundary loop of I.2 over the full-width
// int32 accumulator — b1, the 2w window contributions, the active dictionary
// columns, relu, the output dot in int64 plus the b2 offset (I.1-(5)).
inline void score_i32(const Model& model, const EncodedEgc& enc, Workspace& ws,
                      std::vector<std::int32_t>& out) {
    const Config& c = model.config();
    const std::size_t m = enc.egc_count();
    const std::size_t boundaries = m - 1;
    const std::size_t h_dim = c.hidden;
    const std::size_t slots = 2u * c.char_window;
    const PrecomputeTable& precompute = model.precompute();
    const Span<const std::int32_t> b1_q = model.b1_q();
    const Span<const std::int16_t> w2 = model.w2();

    ws.acc.resize(h_dim);
    std::int32_t* acc = ws.acc.data();
    for (std::size_t i = 0; i < boundaries; ++i) {
        std::copy(b1_q.begin(), b1_q.end(), acc);
        for (std::size_t j = 0; j < slots; ++j) {
            precompute.add_into(slot_rows(enc, m, i, j, c.char_window), j, acc);
        }
        for (std::uint32_t o = ws.dict.offsets[i]; o < ws.dict.offsets[i + 1]; ++o) {
            kernels::add_i32(model.dict_col(ws.dict.indices[o]).data(), acc, h_dim);
        }
        kernels::relu_i32(acc, h_dim);
        out.push_back(saturate(kernels::dot_i32(w2.data(), acc, h_dim) + model.b2_q()));
    }
}

// The Int16 production path (design.ja.md 4.6), fused: each boundary's whole
// hidden-unit pass — b1 init, the 2w window-slot saturating adds, active dict
// columns, relu, and the output dot — runs in one register-resident sweep
// (kernels::fused_score_i16) instead of ~2w+3 separate full-width passes over
// the accumulator. Bit-identical to the unfused sequence: window slots are
// gathered in order (fallback EGCs synthesized into per-slot scratch), dict
// columns appended after, and fused_score_i16 saturating-adds them in that
// same order. Only the redundant accumulator round-trips through memory are
// removed (the profiled bottleneck), not the arithmetic.
inline void score_i16_fused(const Model& model, const EncodedEgc& enc, Workspace& ws,
                            std::vector<std::int32_t>& out) {
    const Config& c = model.config();
    const std::size_t m = enc.egc_count();
    const std::size_t boundaries = m - 1;
    const std::size_t h_dim = c.hidden;
    const std::size_t slots = 2u * c.char_window;
    const PrecomputeTable& precompute = model.precompute();
    const std::int16_t* b1 = model.b1_q16().data();
    const std::int16_t* w2 = model.w2().data();
    const std::int64_t b2 = model.b2_q16();

    // One scratch slot (H) per window position, not a single shared H buffer:
    // block_i16 may return a pointer into this buffer, and all `slots` block
    // pointers stay live in ws.blocks until fused_score_i16 reads them at the
    // end of the boundary — so two fallback EGCs sharing one buffer would alias
    // and corrupt each other.
    ws.fallback.resize(slots * h_dim);
    for (std::size_t i = 0; i < boundaries; ++i) {
        ws.blocks.clear();
        for (std::size_t j = 0; j < slots; ++j) {
            ws.blocks.push_back(
                precompute.block_i16(slot_rows(enc, m, i, j, c.char_window), j,
                                     ws.fallback.data() + j * h_dim));
        }
        for (std::uint32_t o = ws.dict.offsets[i]; o < ws.dict.offsets[i + 1]; ++o) {
            ws.blocks.push_back(model.dict_col16(ws.dict.indices[o]).data());
        }
        const std::int64_t s =
            kernels::fused_score_i16(b1, ws.blocks.data(), ws.blocks.size(), w2, h_dim);
        out.push_back(saturate(s + b2));
    }
}

}  // namespace detail

// Scores every EGC boundary of `enc` (M-1 scores for M clusters): the int16
// integer forward pass of design.ja.md 4.6,
// through the precision path the model was loaded with (model.h). A boundary
// is predicted where the score is positive. Scores are the int64 output sum
// saturated to int32 (the sign — the only thing inference uses — is
// preserved; kytea::scorer's int32 score shape is kept for uniformity).
// Int16-mode scores are in a 2^kAccShift coarser unit than Int32-mode ones;
// only their signs are comparable across the two modes.
inline void score_boundaries_into(const Model& model, const EncodedEgc& enc,
                                  Workspace& ws, std::vector<std::int32_t>& out) {
    const std::size_t m = enc.egc_count();
    out.clear();
    if (m < 2) {
        return;
    }
    out.reserve(m - 1);
    model.dict().features_into(enc, ws.dict);

    if (model.table_precision() == TablePrecision::Int32) {
        detail::score_i32(model, enc, ws, out);
    } else {
        detail::score_i16_fused(model, enc, ws, out);
    }
}

[[nodiscard]] inline std::vector<std::int32_t> score_boundaries(const Model& model,
                                                                const EncodedEgc& enc) {
    Workspace ws;
    std::vector<std::int32_t> out;
    score_boundaries_into(model, enc, ws, out);
    return out;
}

}  // namespace segmentlib::mlp
