#pragma once

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "segmentlib/support/expected.h"
#include "segmentlib/support/span.h"
#include "segmentlib/types.h"
#include "segmentlib/unicode/egc.h"
#include "segmentlib/unicode/normalize.h"
#include "segmentlib/unicode/utf8.h"

namespace segmentlib::mlp {

// Reserved embedding rows (design.ja.md 4.7 field 9): row 0 is the PAD token
// (window positions past the ends of the text), row 1 is UNK (codepoints not
// in the vocabulary). Rows 2.. correspond to the model's sorted codepoint
// array in order.
inline constexpr std::uint32_t kPadRow = 0;
inline constexpr std::uint32_t kUnkRow = 1;

// An input string encoded for the MLP scorer: normalized, split into
// extended grapheme clusters, each cluster's constituent codepoints mapped to
// embedding row ids. The row-id lists are variable-length (almost always one
// codepoint per EGC in Japanese/Chinese), so they are stored CSR-style:
// cluster i owns rows[egc_starts[i] .. egc_starts[i+1]).
//
// `offsets` has size egc_count()+1 and records the byte span of each cluster
// in the *original* (un-normalized) input, so words can be sliced out of the
// source text. Normalization is codepoint-to-codepoint, so original byte
// spans are well-defined even though row ids reflect normalized codepoints.
struct EncodedEgc {
    std::vector<std::uint32_t> rows;        // constituent-codepoint row ids
    std::vector<char32_t> cps;              // normalized codepoints, ∥ rows
    std::vector<std::uint32_t> egc_starts;  // size egc_count()+1, CSR into rows
    std::vector<std::size_t> offsets;       // size egc_count()+1, original bytes

    // Number of EGCs (M). Boundary candidates are the M-1 inter-cluster gaps.
    [[nodiscard]] std::size_t egc_count() const noexcept {
        return egc_starts.empty() ? 0 : egc_starts.size() - 1;
    }

    // Row ids of cluster i's constituent codepoints.
    [[nodiscard]] Span<const std::uint32_t> egc_rows(std::size_t i) const noexcept {
        return Span<const std::uint32_t>(rows.data() + egc_starts[i],
                                         egc_starts[i + 1] - egc_starts[i]);
    }

    // Normalized codepoints of cluster i. Row ids alias distinct codepoints
    // under UNK, so consumers that need the identity of a cluster — the
    // dictionary matcher's EGC interning (5.4) — key on these instead.
    [[nodiscard]] std::u32string_view egc_cps(std::size_t i) const noexcept {
        return std::u32string_view(cps.data() + egc_starts[i],
                                   egc_starts[i + 1] - egc_starts[i]);
    }
};

// The MLP model's codepoint vocabulary (design.ja.md 4.3/4.7): maps a
// codepoint to its embedding row. Corresponds to kytea::CharTable, but the
// id space is embedding rows with reserved PAD/UNK rows, and there is no
// character-type channel (5.1: no heuristic features).
class Vocab {
public:
    // An empty vocabulary: every codepoint maps to UNK.
    Vocab() : Vocab(std::vector<char32_t>{}) {}

    // Takes the model's codepoint array (5.7 field 10): strictly ascending,
    // codepoint i mapping to row i+2. The loader validates the ordering when
    // parsing the model file; this constructor asserts it in debug builds.
    explicit Vocab(std::vector<char32_t> sorted_codepoints)
        : codepoints_(std::move(sorted_codepoints)) {
        assert(std::is_sorted(codepoints_.begin(), codepoints_.end()) &&
               std::adjacent_find(codepoints_.begin(), codepoints_.end()) ==
                   codepoints_.end());

        bmp_rows_.assign(kBmpSize, kUnkRow);
        for (std::size_t i = 0; i < codepoints_.size(); ++i) {
            if (codepoints_[i] < kBmpSize) {
                bmp_rows_[codepoints_[i]] = static_cast<std::uint32_t>(i) + 2;
            }
        }
    }

    // Embedding row for a codepoint; kUnkRow if it is not in the vocabulary.
    // BMP codepoints (the common case) resolve through a direct-indexed
    // table; only rarer astral codepoints fall back to binary search.
    [[nodiscard]] std::uint32_t row_of(char32_t cp) const noexcept {
        return cp < kBmpSize ? bmp_rows_[cp] : row_of_astral(cp);
    }

    // Number of embedding rows V, including the PAD and UNK rows.
    [[nodiscard]] std::uint32_t size() const noexcept {
        return static_cast<std::uint32_t>(codepoints_.size()) + 2;
    }

    // The ascending codepoint array (5.7 field 10): codepoints()[i]
    // corresponds to embedding row i+2. The model exporter writes this out
    // verbatim.
    [[nodiscard]] Span<const char32_t> codepoints() const noexcept {
        return codepoints_;
    }

    // Same as encode(), but fills a caller-owned EncodedEgc (its buffers are
    // reused, avoiding per-call allocation on the hot path). On error the
    // contents of `out` are unspecified.
    [[nodiscard]] Expected<void, Error> encode_into(std::string_view utf8,
                                                    EncodedEgc& out) const {
        out.rows.clear();
        out.cps.clear();
        out.egc_starts.clear();
        out.offsets.clear();

        // Normalization is applied per codepoint *before* cluster segmentation
        // (design.ja.md 4.5 方式(a)), so the breaker sees normalized codepoints;
        // offsets still index the original bytes (normalization is 1:1 on
        // codepoints, so cluster spans carry over).
        unicode::GraphemeBreaker breaker;
        std::size_t pos = 0;
        while (pos < utf8.size()) {
            const auto decoded = unicode::decode(utf8.substr(pos));
            if (!decoded) {
                return Unexpected(Error{ErrorCode::InvalidUtf8, "invalid UTF-8 in input"});
            }
            const char32_t norm = unicode::normalize(decoded->codepoint);
            if (breaker.next(norm)) {
                out.egc_starts.push_back(static_cast<std::uint32_t>(out.rows.size()));
                out.offsets.push_back(pos);
            }
            out.rows.push_back(row_of(norm));
            out.cps.push_back(norm);
            pos += decoded->length;
        }
        out.egc_starts.push_back(static_cast<std::uint32_t>(out.rows.size()));
        out.offsets.push_back(utf8.size());
        return {};
    }

    // Encodes UTF-8 input: normalization (unicode::normalize, 方式(a)) →
    // EGC segmentation (unicode::egc) → embedding row ids. Returns
    // InvalidUtf8 on malformed input.
    [[nodiscard]] Expected<EncodedEgc, Error> encode(std::string_view utf8) const {
        EncodedEgc out;
        if (auto r = encode_into(utf8, out); !r) {
            return Unexpected(r.error());
        }
        return out;
    }

private:
    static constexpr char32_t kBmpSize = 0x1'0000;

    [[nodiscard]] std::uint32_t row_of_astral(char32_t cp) const noexcept {
        const auto it = std::lower_bound(codepoints_.begin(), codepoints_.end(), cp);
        if (it != codepoints_.end() && *it == cp) {
            return static_cast<std::uint32_t>(it - codepoints_.begin()) + 2;
        }
        return kUnkRow;
    }

    std::vector<char32_t> codepoints_;      // ascending; row i+2 <-> codepoints_[i]
    std::vector<std::uint32_t> bmp_rows_;   // size kBmpSize, direct-indexed
};

}  // namespace segmentlib::mlp
