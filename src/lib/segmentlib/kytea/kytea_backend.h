#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>
#include <utility>
#include <vector>

#include "segmentlib/kytea/model.h"
#include "segmentlib/kytea/scorer.h"
#include "segmentlib/support/expected.h"
#include "segmentlib/types.h"

namespace segmentlib::kytea {

namespace detail {

// True if a boundary with the given raw score should be cut. KyTea places a
// boundary where score * multiplier > 0 (confidence threshold 0).
inline bool is_cut(std::int32_t score, double multiplier) noexcept {
    return static_cast<double>(score) * multiplier > 0.0;
}

// Per-thread scratch reused across calls so the hot path allocates only the
// returned result, not the intermediate encoding/score buffers. thread_local
// keeps tokenize re-entrant (the Model is immutable and shared).
//
// The buffers live inside an accessor rather than at namespace scope so that
// tokenize(), which is inline, refers to one entity program-wide: a
// namespace-scope thread_local in a header would be a separate object in every
// translation unit that included it.
struct Scratch {
    EncodedText enc;
    std::vector<std::int32_t> scores;
};

inline Scratch& scratch() {
    thread_local Scratch s;
    return s;
}

}  // namespace detail

// The KyTea-compatible segmentation backend: holds a loaded Model and turns
// text into words using the pointwise scorer. Satisfies the backend interface
// the Segmenter dispatches to. Word segmentation only — no tag prediction.
class KyteaBackend {
public:
    explicit KyteaBackend(Model model) noexcept : model_(std::move(model)) {}

    [[nodiscard]] Expected<Segments, Error> tokenize(std::string_view text) const {
        detail::Scratch& s = detail::scratch();
        if (auto r = model_.chars().encode_into(text, s.enc); !r) {
            return Unexpected(r.error());
        }
        const std::size_t n = s.enc.length();
        Segments segments;
        if (n == 0) {
            return segments;
        }
        score_boundaries_into(model_, s.enc, s.scores);
        const double mult = model_.multiplier();
        std::size_t start = 0;
        for (std::size_t i = 0; i < n; ++i) {
            const bool cut = (i + 1 < n) ? detail::is_cut(s.scores[i], mult) : true;
            if (cut) {
                segments.emplace_back(s.enc.offsets[start], s.enc.offsets[i + 1]);
                start = i + 1;
            }
        }
        return segments;
    }

    [[nodiscard]] const Model& model() const noexcept { return model_; }

private:
    Model model_;
};

}  // namespace segmentlib::kytea
