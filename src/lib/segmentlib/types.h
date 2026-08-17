#pragma once

#include <cstddef>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace segmentlib {

// Word-segmentation output: one (start, end) byte-offset pair per word, into
// the original UTF-8 input. start is inclusive, end exclusive; consecutive
// pairs share an endpoint (pairs[i].second == pairs[i + 1].first), with the
// first pair starting at 0 and the last ending at the input size.
using Segments = std::vector<std::pair<std::size_t, std::size_t>>;

enum class ErrorCode {
    InvalidUtf8,
    ModelNotLoaded,
    UnsupportedModelFormat,
    MalformedModel,
    MalformedCorpus,
    IoError,
};

// A failure result. `message` points at a static string (no dynamic storage),
// so an Error can be copied and returned freely.
struct Error {
    ErrorCode code;
    std::string_view message;
};

}  // namespace segmentlib
