#pragma once

#include <filesystem>
#include <string_view>
#include <utility>

#include "segmentlib/mlp/model.h"
#include "segmentlib/support/expected.h"
#include "segmentlib/support/span.h"
#include "segmentlib/types.h"

namespace segmentlib::ed {

// The header-line signature used by backend auto-detection; the full first line
// is "SegmentLibED <version>\n". Note it is one byte shorter than the MLP
// signature, which is why Segmenter::load reads the longest signature it knows
// and compares each candidate over its own length.
inline constexpr std::string_view kModelSignature = "SegmentLibED ";

inline constexpr mlp::FormatId kEdFormat{
    kModelSignature,
    "1",
    "not a SegmentLibED model",
    "unsupported SegmentLibED version",
    "malformed SegmentLibED model",
};

// An immutable loaded EDLA model.
//
// The body behind the header line is the MLP layout byte for byte, and the
// scoring path is the MLP one: EDLA (arXiv 2504.14814) changes how the weights
// are learned — a single global error signal broadcast to every layer, with
// each unit's update built from its own activity instead of a backpropagated
// gradient — not what the trained network computes. So this type owns an
// mlp::Model and adds nothing to it; it exists to give the format its own
// identity, so a file says which rule trained it and the two never load as each
// other. Sharing the numeric core is deliberate: with the network shape held
// fixed, any measured difference between the backends is the learning rule,
// which is the entire point of having both (design.md 11).
class Model {
public:
    [[nodiscard]] static Expected<Model, Error> load_from_bytes(
        Span<const std::byte> data,
        mlp::TablePrecision precision = mlp::TablePrecision::Int16) {
        return wrap(mlp::Model::load_format(data, kEdFormat, precision));
    }

    [[nodiscard]] static Expected<Model, Error> load(
        const std::filesystem::path& path,
        mlp::TablePrecision precision = mlp::TablePrecision::Int16) {
        return wrap(mlp::Model::load_format(path, kEdFormat, precision));
    }

    // The scoring network. Everything a caller might ask of the model —
    // config, vocabulary, quantized layers — is reached through here rather
    // than re-exported, so there is no wrapper to drift from what it wraps.
    [[nodiscard]] const mlp::Model& net() const noexcept { return net_; }

private:
    static Expected<Model, Error> wrap(Expected<mlp::Model, Error> net) {
        if (!net) {
            return Unexpected(net.error());
        }
        return Model(std::move(*net));
    }

    explicit Model(mlp::Model net) noexcept : net_(std::move(net)) {}

    mlp::Model net_;
};

}  // namespace segmentlib::ed
