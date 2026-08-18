#pragma once

// The library version, in the same two spellings cpp-httplib and cpp-peglib
// use: a display string and a hex number (0xMMmmpp) for preprocessor
// comparison. Kept here because types.h is the dependency-free header every
// other segmentlib header includes. Releases bump these together with the
// VERSION in CMakeLists.txt's project() -- see docs/RELEASING.md.
#define SEGMENTLIB_VERSION "0.1.0"
#define SEGMENTLIB_VERSION_NUM "0x000100"

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
