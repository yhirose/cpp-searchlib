#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "segmentlib/support/endian.h"
#include "segmentlib/support/span.h"

namespace segmentlib::bytes {

// The writing counterpart of BinaryReader: appends values to an in-memory
// byte buffer in exactly the encoding BinaryReader consumes, so a structure
// written here round-trips through the reader byte for byte.
//
// Encoding rules (mirroring BinaryReader):
//  - integers are fixed-width little-endian regardless of host endianness;
//  - other trivially-copyable values (notably double) are raw host bytes —
//    on our little-endian targets that is little-endian on disk, and
//    big-endian hosts are a known deferred concern shared with the reader
//    (design.ja.md 4.7);
//  - strings are NUL-terminated, header lines '\n'-terminated.
//
// Used by the model exporters (e.g. the MLP trainer, design.ja.md 4.7). The
// buffer is accumulated in memory and handed to the caller with data()/take();
// model files are small enough (tens of MB) that this beats streaming I/O in
// simplicity without a meaningful memory cost.
class BinaryWriter {
public:
    [[nodiscard]] Span<const std::byte> data() const noexcept { return buf_; }
    [[nodiscard]] std::size_t size() const noexcept { return buf_.size(); }

    // Moves the accumulated buffer out; the writer is empty afterwards.
    [[nodiscard]] std::vector<std::byte> take() noexcept { return std::move(buf_); }

    // Writes a trivially-copyable value. Integers wider than a byte are
    // encoded as little-endian (the inverse of BinaryReader::read<T>()).
    template <class T>
    void write(T value) {
        static_assert(std::is_trivially_copyable_v<T>);
        if constexpr (std::is_integral_v<T> && sizeof(T) > 1) {
            if constexpr (kNativeIsBigEndian) {
                value = byteswap(value);
            }
        }
        const std::size_t pos = buf_.size();
        buf_.resize(pos + sizeof(T));
        std::memcpy(buf_.data() + pos, &value, sizeof(T));
    }

    void write_bool(bool b) { write<std::uint8_t>(b ? 1 : 0); }

    // Writes a NUL-terminated string (the form BinaryReader::read_cstring
    // consumes). `s` must not itself contain a NUL, or the reader would stop
    // short; model strings (words, header fields) never do.
    void write_cstring(std::string_view s) {
        write_bytes(as_bytes(Span<const char>(s)));
        write<std::uint8_t>(0);
    }

    // Writes `line` followed by '\n' (the form BinaryReader::read_line
    // consumes). `line` must not contain '\n'.
    void write_line(std::string_view line) {
        write_bytes(as_bytes(Span<const char>(line)));
        write<std::uint8_t>('\n');
    }

    // Appends raw bytes as-is (e.g. a whole int16 tensor already in
    // little-endian order).
    void write_bytes(Span<const std::byte> bytes) {
        buf_.insert(buf_.end(), bytes.begin(), bytes.end());
    }

private:
    std::vector<std::byte> buf_;
};

}  // namespace segmentlib::bytes
