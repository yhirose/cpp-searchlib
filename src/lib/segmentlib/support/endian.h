#pragma once

#include <cstddef>
#include <cstdint>
#include <type_traits>

// std::endian and std::byteswap are C++20/23; this is the C++17 equivalent of
// the two things the binary reader and writer ask of them.

#if defined(__BYTE_ORDER__) && defined(__ORDER_BIG_ENDIAN__)
#define SEGMENTLIB_IS_BIG_ENDIAN (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
#elif defined(_MSC_VER)
// MSVC targets (x86, x64, ARM/ARM64 in little-endian mode) are all
// little-endian, and MSVC defines no byte-order macro to test.
#define SEGMENTLIB_IS_BIG_ENDIAN 0
#else
#error "segmentlib: cannot determine the target byte order"
#endif

namespace segmentlib {

inline constexpr bool kNativeIsBigEndian = SEGMENTLIB_IS_BIG_ENDIAN != 0;

// Reverses the bytes of an integer. Written as a loop rather than as the
// per-width compiler builtins because it only ever runs on a big-endian host —
// the model formats are little-endian on disk — so clarity beats the
// single-instruction lowering, and this stays constexpr on every compiler.
template <class T>
[[nodiscard]] constexpr T byteswap(T value) noexcept {
    static_assert(std::is_integral_v<T>, "byteswap requires an integral type");
    using U = std::make_unsigned_t<T>;
    auto in = static_cast<U>(value);
    U out = 0;
    for (std::size_t i = 0; i < sizeof(T); ++i) {
        out = static_cast<U>(static_cast<U>(out << 8) | static_cast<U>(in & 0xFF));
        in = static_cast<U>(in >> 8);
    }
    return static_cast<T>(out);
}

}  // namespace segmentlib
