#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>
#include <utility>
#include <vector>

#include "segmentlib/mlp/model.h"
#include "segmentlib/mlp/scorer.h"
#include "segmentlib/support/expected.h"
#include "segmentlib/types.h"

namespace segmentlib::mlp {

namespace detail {

// Per-thread scratch (I.5): the encoder output, scorer workspace and score
// buffer are reused across calls without allocation, and each thread of a
// tokenize_all-style parallel batch gets its own set (the Model itself is
// immutable and shared). Sharing across backend instances on one thread is
// fine — every buffer is cleared or overwritten per call.
struct Scratch {
    EncodedEgc enc;
    Workspace ws;
    std::vector<std::int32_t> scores;
};

inline Scratch& scratch() {
    thread_local Scratch s;
    return s;
}

}  // namespace detail

// The MLP segmentation backend: same shape as
// kytea::KyteaBackend. Word segmentation only.
class MlpBackend {
public:
    explicit MlpBackend(Model model) noexcept : model_(std::move(model)) {}

    [[nodiscard]] Expected<Segments, Error> tokenize(std::string_view text) const {
        detail::Scratch& s = detail::scratch();
        if (auto encoded = model_.vocab().encode_into(text, s.enc); !encoded) {
            return Unexpected(encoded.error());
        }
        Segments segments;
        if (text.empty()) {
            return segments;
        }
        score_boundaries_into(model_, s.enc, s.ws, s.scores);
        std::size_t start = 0;
        for (std::size_t i = 0; i < s.scores.size(); ++i) {
            if (s.scores[i] > 0) {
                // Boundary i sits after EGC i: the byte offset where EGC i+1
                // starts in the original input.
                const std::size_t cut = s.enc.offsets[i + 1];
                segments.emplace_back(start, cut);
                start = cut;
            }
        }
        segments.emplace_back(start, text.size());
        return segments;
    }

    [[nodiscard]] const Model& model() const noexcept { return model_; }

private:
    Model model_;
};

}  // namespace segmentlib::mlp
