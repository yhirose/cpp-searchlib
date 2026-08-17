#pragma once

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <vector>

#include "segmentlib/mlp/kernels.h"
#include "segmentlib/support/span.h"

namespace segmentlib::mlp {

// Which numeric representation the first-layer table and accumulator use
// (design.ja.md 4.6):
//  - Int32: the first implementation — no clipping anywhere, bit-exact with
//    the trainer's int16 reference forward (train/quantize.cpp). The
//    verification path.
//  - Int16: the requantized table (5.6の後段最適化, NNUE's form): every
//    accumulator-scale int32 value is shifted right by kAccShift with
//    rounding and saturated to int16, and accumulation uses saturating int16
//    adds — half the memory and roughly double the SIMD throughput. Rare
//    saturations / decision flips are possible near the margin and are
//    measured against the Int32 path (I.3: 飽和発生率と判定反転をチェック).
enum class TablePrecision : std::uint8_t { Int32, Int16 };

// The int16 accumulator unit is S_acc·2^kAccShift. S_acc calibration maps
// pct99.99(|a|) to Amax = 2^22 (I.1), so the shift lands that point on
// 2^13 = 8192 — a 4× headroom inside int16 for the summation beyond the
// percentile. NOTE: this presumes a properly calibrated acc_scale; a model
// whose acc_scale is not derived from activation statistics will clip.
inline constexpr int kAccShift = 9;

// The single int32 → int16 requantization used for *every* Int16-mode
// quantity (table blocks, fallback contributions, dictionary columns, b1):
// round-to-nearest arithmetic shift, then saturate.
[[nodiscard]] constexpr std::int16_t requant_i16(std::int32_t v) noexcept {
    const std::int32_t shifted = (v + (1 << (kAccShift - 1))) >> kAccShift;
    return static_cast<std::int16_t>(std::clamp(shifted, -32768, 32767));
}

// The first-layer precompute table of design.ja.md 4.6 (NNUE style):
// table[egc][j] = W1_j · v(egc) in the accumulator integer scale S_acc
// built at load time from the quantized
// embedding and W1.
//
// The first implementation's frequent-EGC set (I.4): every single-codepoint
// EGC — i.e. every embedding row, including the PAD (row 0) and UNK (row 1)
// pseudo-entries — has a table block; multi-codepoint EGCs take the fallback
// synthesis path (I.1-(2)), which uses the identical integer computation, so
// the table is a pure cache: both paths are bit-exact by construction (in
// Int16 mode both go through the same requant_i16, preserving the property).
//
// Memory: V × 2w × H × 4 bytes in Int32 mode (~100MB at V=10k/w=5/H=256),
// half that in Int16 mode.
class PrecomputeTable {
public:
    PrecomputeTable() = default;

    // Builds the table. `emb_q` is V×d, `w1_q` is H×(2w·d), both in the 5.7
    // layout; `r` is the rescale factor R = S_e·S_w1 / S_acc. The table
    // keeps copies of `emb_q`/`w1_q` for the fallback path, so it does not
    // borrow from the caller.
    PrecomputeTable(Span<const std::int16_t> emb_q, Span<const std::int16_t> w1_q,
                    std::uint32_t vocab_size, std::uint16_t embed_dim,
                    std::uint16_t hidden, std::uint8_t window, double r,
                    TablePrecision precision = TablePrecision::Int32)
        : emb_q_(emb_q.begin(), emb_q.end()),
          w1_q_(w1_q.begin(), w1_q.end()),
          d_(embed_dim),
          h_(hidden),
          window_(window),
          r_(r),
          precision_(precision) {
        const std::size_t slots = 2u * window_;
        const std::size_t entries = static_cast<std::size_t>(vocab_size) * slots * h_;
        if (precision_ == TablePrecision::Int32) {
            table32_.resize(entries);
        } else {
            table16_.resize(entries);
        }

        std::uint32_t single_row[1];
        for (std::uint32_t row = 0; row < vocab_size; ++row) {
            single_row[0] = row;  // a single-codepoint EGC (n = 1)
            for (std::size_t j = 0; j < slots; ++j) {
                const std::size_t base =
                    (static_cast<std::size_t>(row) * slots + j) * h_;
                if (precision_ == TablePrecision::Int32) {
                    synthesize(Span<const std::uint32_t>(single_row, 1), j,
                               [&](std::size_t h, std::int32_t v) {
                                   table32_[base + h] = v;
                               });
                } else {
                    synthesize(Span<const std::uint32_t>(single_row, 1), j,
                               [&](std::size_t h, std::int32_t v) {
                                   table16_[base + h] = requant_i16(v);
                               });
                }
            }
        }
    }

    [[nodiscard]] TablePrecision precision() const noexcept { return precision_; }

    // acc[h] += table-or-fallback contribution of the EGC whose constituent
    // rows are `rows`, at window position j (I.2). `acc` must have H entries.
    // Single-row EGCs (and PAD, rows = {0}) hit the table; multi-row EGCs
    // synthesize. Int32 mode only — the Int16 path gathers via block_i16.
    void add_into(Span<const std::uint32_t> rows, std::size_t j,
                  std::int32_t* acc) const {
        assert(precision_ == TablePrecision::Int32);
        if (rows.size() == 1) {
            const std::int32_t* block =
                table32_.data() +
                (static_cast<std::size_t>(rows[0]) * (2u * window_) + j) * h_;
            kernels::add_i32(block, acc, h_);
            return;
        }
        // Fallback synthesis (I.1-(2)) for multi-codepoint EGCs.
        synthesize(rows, j, [&](std::size_t h, std::int32_t v) { acc[h] += v; });
    }

    // Returns the H-length Int16 contribution block for the EGC `rows` at window
    // slot j, for the fused scorer (kernels::fused_score_i16). Single-row EGCs
    // (and PAD) return a pointer straight into the table; multi-row EGCs
    // synthesize into `scratch` (which must hold >= H int16) and return it. The
    // Int16 counterpart of add_into.
    [[nodiscard]] const std::int16_t* block_i16(Span<const std::uint32_t> rows,
                                                std::size_t j,
                                                std::int16_t* scratch) const {
        assert(precision_ == TablePrecision::Int16);
        if (rows.size() == 1) {
            return table16_.data() +
                   (static_cast<std::size_t>(rows[0]) * (2u * window_) + j) * h_;
        }
        // Fallback (I.1-(2)) for multi-codepoint EGCs: the int32 contribution
        // requantized exactly as the table entries were, written to scratch so the
        // fused scorer can saturating-add it in window order — bit-identical to the
        // table path for the same EGC.
        synthesize(rows, j,
                   [&](std::size_t h, std::int32_t v) { scratch[h] = requant_i16(v); });
        return scratch;
    }

private:
    // Per-h fallback contribution (the shared integer kernel of I.1-(2));
    // callers requantize per mode.
    //
    // The shared integer kernel of I.1-(1)/(2): given the constituent rows of
    // one EGC, compute every hidden unit's contribution at window position j:
    //   sum_c[k] = Σ_rows emb_q[row, k]                        (int32)
    //   raw[h]   = Σ_k w1_q[h, j*d+k] · sum_c[k]               (int64)
    //   emit(h, llround(raw[h] · r / n))                        (int32)
    // Table construction, the Int32 fallback and the Int16 fallback all go
    // through this one function, which is what makes table and fallback bit-exact
    // with each other (and, in Int32 mode, with the trainer's reference forward).
    template <class Emit>
    void synthesize(Span<const std::uint32_t> rows, std::size_t j, Emit emit) const {
        // int64, not int32: `rows` is one EGC's constituent codepoints, and an EGC
        // is caller-controlled — a long run of combining marks is a single cluster
        // of unbounded length. At int32 a ~400 KB line of them overflowed the
        // accumulator (UB), so the width is a correctness requirement, not
        // headroom. Values that fit in int32 accumulate identically here, so the
        // bit-exactness contract with the trainer's reference forward is unchanged.
        std::int64_t sum_c[256];  // the loader rejects embed_dim > 256
        const std::size_t d = d_;
        assert(d <= std::size(sum_c));
        for (std::size_t k = 0; k < d; ++k) {
            std::int64_t s = 0;
            for (const std::uint32_t row : rows) {
                s += emb_q_[static_cast<std::size_t>(row) * d + k];
            }
            sum_c[k] = s;
        }
        const std::size_t in_dim = 2u * window_ * d;
        const auto n = static_cast<double>(rows.size());
        for (std::size_t h = 0; h < h_; ++h) {
            const std::int16_t* w1_row = w1_q_.data() + h * in_dim + j * d;
            std::int64_t raw = 0;
            for (std::size_t k = 0; k < d; ++k) {
                raw += static_cast<std::int64_t>(w1_row[k]) * sum_c[k];
            }
            emit(h, static_cast<std::int32_t>(
                        std::llround(static_cast<double>(raw) * r_ / n)));
        }
    }

    std::vector<std::int32_t> table32_;  // [row][j][h], Int32 mode
    std::vector<std::int16_t> table16_;  // [row][j][h], Int16 mode
    std::vector<std::int16_t> emb_q_;    // V × d (fallback)
    std::vector<std::int16_t> w1_q_;     // H × 2w·d (fallback)
    std::uint16_t d_ = 0;
    std::uint16_t h_ = 0;
    std::uint8_t window_ = 0;
    double r_ = 0.0;
    TablePrecision precision_ = TablePrecision::Int32;
};

}  // namespace segmentlib::mlp
