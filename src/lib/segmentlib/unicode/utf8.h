#pragma once

#include <cstddef>
#include <optional>
#include <string>
#include <string_view>

namespace segmentlib::unicode {

// Number of bytes in the UTF-8 sequence introduced by `lead`, or 0 if `lead`
// is not a valid leading byte.
constexpr int sequence_length(unsigned char lead) noexcept {
    if (lead < 0x80) return 1;
    if ((lead >> 5) == 0b110) return 2;
    if ((lead >> 4) == 0b1110) return 3;
    if ((lead >> 3) == 0b11110) return 4;
    return 0;
}

struct Decoded {
    char32_t codepoint;
    std::size_t length;  // bytes consumed
};

namespace detail {

constexpr bool is_continuation(unsigned char b) noexcept {
    return (b >> 6) == 0b10;
}

// Smallest codepoint legally encodable in a sequence of the given length.
// Used to reject overlong encodings. Index by sequence length (1..4).
//
// `inline` matters: a plain namespace-scope constexpr array has internal
// linkage, so every translation unit including this header would get its own,
// and the inline functions below reading it would no longer share one
// definition across the program.
inline constexpr char32_t kMinCodepoint[5] = {0, 0x0, 0x80, 0x800, 0x10000};

}  // namespace detail

// Decodes the first codepoint of `s`. Returns nullopt on malformed input:
// invalid leading byte, truncated sequence, bad continuation byte, overlong
// encoding, surrogate, or out-of-range codepoint.
inline std::optional<Decoded> decode(std::string_view s) noexcept {
    if (s.empty()) {
        return std::nullopt;
    }
    const auto lead = static_cast<unsigned char>(s[0]);
    const int len = sequence_length(lead);
    if (len == 0 || static_cast<std::size_t>(len) > s.size()) {
        return std::nullopt;
    }

    char32_t cp = 0;
    switch (len) {
        case 1: cp = lead; break;
        case 2: cp = lead & 0x1F; break;
        case 3: cp = lead & 0x0F; break;
        default: cp = lead & 0x07; break;
    }
    for (int i = 1; i < len; ++i) {
        const auto b = static_cast<unsigned char>(s[i]);
        if (!detail::is_continuation(b)) {
            return std::nullopt;
        }
        cp = (cp << 6) | (b & 0x3F);
    }

    // Reject overlong encodings, surrogates, and out-of-range codepoints.
    if (cp < detail::kMinCodepoint[len] || cp > 0x10FFFF ||
        (cp >= 0xD800 && cp <= 0xDFFF)) {
        return std::nullopt;
    }
    return Decoded{cp, static_cast<std::size_t>(len)};
}

// Appends the UTF-8 encoding of `cp` to `out`. `cp` must be a valid Unicode
// scalar value (<= U+10FFFF, not a surrogate); callers pass codepoints that came
// from decode() or a model's character table, so no validation is done here.
inline void encode(char32_t cp, std::string& out) {
    if (cp < 0x80) {
        out.push_back(static_cast<char>(cp));
    } else if (cp < 0x800) {
        out.push_back(static_cast<char>(0xC0 | (cp >> 6)));
        out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
    } else if (cp < 0x1'0000) {
        out.push_back(static_cast<char>(0xE0 | (cp >> 12)));
        out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
    } else {
        out.push_back(static_cast<char>(0xF0 | (cp >> 18)));
        out.push_back(static_cast<char>(0x80 | ((cp >> 12) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
    }
}

}  // namespace segmentlib::unicode
