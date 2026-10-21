#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <type_traits>

#include "segmentlib/support/endian.h"
#include "segmentlib/support/span.h"

namespace segmentlib::bytes {

// Thrown on any attempt to read past the end of the buffer or on a malformed
// low-level structure. It is caught once, at the model-loading boundary, and
// converted into a segmentlib::Error there — so the rest of the parser can be
// written straight-line without threading error codes through every call.
class ParseError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

// A forward-only cursor over an in-memory byte buffer.
//
// KyTea's binary model writes native-endian fixed-width integers; on the
// little-endian platforms we target that is little-endian on disk. read<T>()
// interprets integers as little-endian regardless of host endianness, so the
// reader stays correct even on a big-endian host.
class BinaryReader {
public:
    explicit BinaryReader(Span<const std::byte> data) noexcept : data_(data) {}

    [[nodiscard]] std::size_t position() const noexcept { return pos_; }
    [[nodiscard]] std::size_t remaining() const noexcept { return data_.size() - pos_; }
    [[nodiscard]] bool eof() const noexcept { return pos_ >= data_.size(); }

    // Reads a trivially-copyable value. Integers wider than a byte are decoded
    // as little-endian.
    template <class T>
    [[nodiscard]] T read() {
        static_assert(std::is_trivially_copyable_v<T>);
        T value = read_raw<T>();
        if constexpr (std::is_integral_v<T> && sizeof(T) > 1) {
            if constexpr (kNativeIsBigEndian) {
                value = byteswap(value);
            }
        }
        return value;
    }

    [[nodiscard]] bool read_bool() { return read_raw<std::uint8_t>() != 0; }

    // Reads a NUL-terminated string (KyTea's on-disk KyteaString form). The
    // terminating NUL is consumed but not included in the result.
    [[nodiscard]] std::string read_cstring() {
        const std::size_t end = find_from_pos(std::byte{0});
        if (end == data_.size()) {
            throw ParseError("unterminated C string");
        }
        return take_string(end);
    }

    // Reads up to and including a '\n' (used for the model's header line).
    // The newline is consumed but not included in the result.
    [[nodiscard]] std::string read_line() {
        return take_string(find_from_pos(std::byte{'\n'}));  // else: the rest
    }

    // Reads `n` bytes verbatim. Used for blocks whose interior this reader
    // does not interpret, such as the model's compiled dictionary FST.
    [[nodiscard]] std::string read_blob(std::size_t n) {
        if (n > remaining()) {
            throw ParseError("blob extends past end of buffer");
        }
        std::string out(reinterpret_cast<const char*>(data_.data()) + pos_, n);
        pos_ += n;
        return out;
    }

    void skip(std::size_t n) {
        if (n > remaining()) {
            throw ParseError("skip past end of buffer");
        }
        pos_ += n;
    }

    // Rejects an element count that the bytes left in the buffer cannot
    // possibly back, given that each element occupies at least
    // `min_bytes_each` bytes on disk.
    //
    // read() bounds-checks every read, so a bogus count can never cause an
    // out-of-bounds read — but the container is sized from the count *before*
    // the first element is read, so the check fires only after the allocation.
    // A 60-byte file declaring 2^32-1 vocabulary entries reserved 1.8 GB and
    // spun for 2.5s before reporting the error. Calling this first turns that
    // into an immediate rejection, which is why it is a precondition of every
    // count-driven reserve/resize in the parsers rather than an optional
    // hardening step.
    void require_capacity(std::uint64_t count, std::size_t min_bytes_each) const {
        if (min_bytes_each == 0) {
            return;
        }
        if (count > remaining() / min_bytes_each) {
            throw ParseError("declared element count exceeds the remaining bytes");
        }
    }

private:
    // Index of the first `delim` at or after the cursor, or the buffer size.
    [[nodiscard]] std::size_t find_from_pos(std::byte delim) const noexcept {
        std::size_t end = pos_;
        while (end < data_.size() && data_[end] != delim) {
            ++end;
        }
        return end;
    }

    // The bytes from the cursor up to `end` as a string, with the cursor moved
    // past the delimiter at `end` (if there is one).
    [[nodiscard]] std::string take_string(std::size_t end) {
        std::string out(reinterpret_cast<const char*>(data_.data()) + pos_, end - pos_);
        pos_ = end < data_.size() ? end + 1 : end;
        return out;
    }

    template <class T>
    [[nodiscard]] T read_raw() {
        if (sizeof(T) > remaining()) {
            throw ParseError("read past end of buffer");
        }
        T value;
        std::memcpy(&value, data_.data() + pos_, sizeof(T));
        pos_ += sizeof(T);
        return value;
    }

    Span<const std::byte> data_;
    std::size_t pos_ = 0;
};

}  // namespace segmentlib::bytes
