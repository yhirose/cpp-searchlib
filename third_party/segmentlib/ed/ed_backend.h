#pragma once

#include <string_view>
#include <utility>

#include "segmentlib/ed/model.h"
#include "segmentlib/mlp/mlp_backend.h"
#include "segmentlib/support/expected.h"
#include "segmentlib/types.h"

namespace segmentlib::ed {

// The EDLA segmentation backend: same shape as kytea::KyteaBackend and
// mlp::MlpBackend. Word segmentation only.
//
// Scoring is mlp::tokenize_with, unchanged — an EDLA-trained model differs from
// a backpropagation-trained one in its weights, not in the arithmetic that
// consumes them, so the two backends segment identically given identical
// weights (ed_model_test.cpp pins that).
class EdBackend {
public:
    explicit EdBackend(Model model) noexcept : model_(std::move(model)) {}

    [[nodiscard]] Expected<Segments, Error> tokenize(std::string_view text) const {
        return mlp::tokenize_with(model_.net(), text);
    }

    [[nodiscard]] const Model& model() const noexcept { return model_; }

private:
    Model model_;
};

}  // namespace segmentlib::ed
