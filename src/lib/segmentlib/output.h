#pragma once

#include <cstddef>
#include <string>
#include <string_view>

#include "segmentlib/types.h"

namespace segmentlib {

namespace detail {

// Escapes the delimiter characters KyTea would otherwise treat as structure.
// All four are ASCII, so escaping byte-by-byte never touches a UTF-8
// continuation byte.
inline void append_escaped(std::string_view word, std::string& out) {
    for (const char c : word) {
        if (c == ' ' || c == '/' || c == '&' || c == '\\') {
            out.push_back('\\');
        }
        out.push_back(c);
    }
}

}  // namespace detail

// Appends `segments` to `out` as a KyTea-style word-segmentation line: surface
// words separated by single spaces, with the delimiter characters (space, '/',
// '&', '\') backslash-escaped exactly as KyTea's writeSentence does. No
// trailing newline is added. `original` is the source text the segment
// offsets index.
inline void append_full_line(const Segments& segments, std::string_view original,
                             std::string& out) {
    for (std::size_t i = 0; i < segments.size(); ++i) {
        if (i != 0) {
            out.push_back(' ');
        }
        const auto& [start, end] = segments[i];
        detail::append_escaped(original.substr(start, end - start), out);
    }
}

}  // namespace segmentlib
