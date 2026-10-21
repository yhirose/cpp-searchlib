#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>

// The SIMD kernels of the MLP scoring hot path (design.ja.md 4.6):
//
//   int32 path:  add_i32 (acc += src), relu_i32, dot_i32 (Σ w2·acc → int64)
//   int16 path:  fused_score_i16, the whole per-boundary sequence (b1 init →
//                saturating window and dictionary adds → relu → output dot) in
//                one register-resident sweep
//
// `kernels::scalar` is the always-compiled reference implementation — the
// oracle the SIMD paths are tested against, and the fallback for platforms with
// neither NEON nor AVX2. The unqualified functions dispatch at compile time:
// AArch64 always has NEON; x86 uses AVX2 when the translation unit is built
// with it (-mavx2 / /arch:AVX2), scalar otherwise. Every kernel is bit-exact
// with scalar on every path (integer adds/products are associative, and scalar
// `add_i32` wraps like the SIMD adds). AVX2's `_mm256_madd_epi16` wraps when
// both halves of a pair are -32768, which `fused_score_i16` cannot hit because
// its dot always follows the relu.

// AArch64 only: 32-bit ARM lacks vaddvq_s64 / vmull_high_*.
#if defined(__aarch64__)
#define SEGMENTLIB_KERNELS_NEON 1
#include <arm_neon.h>
#elif defined(__AVX2__)
#define SEGMENTLIB_KERNELS_AVX2 1
#include <immintrin.h>
#endif

namespace segmentlib::mlp::kernels {

namespace scalar {

[[nodiscard]] inline std::int32_t wrapping_add_i32(std::int32_t a, std::int32_t b) noexcept {
    return static_cast<std::int32_t>(static_cast<std::uint32_t>(a) +
                                     static_cast<std::uint32_t>(b));
}

inline void add_i32(const std::int32_t* src, std::int32_t* acc, std::size_t n) {
    for (std::size_t i = 0; i < n; ++i) {
        acc[i] = wrapping_add_i32(acc[i], src[i]);
    }
}

inline void relu_i32(std::int32_t* acc, std::size_t n) {
    for (std::size_t i = 0; i < n; ++i) {
        acc[i] = std::max(acc[i], 0);
    }
}

[[nodiscard]] inline std::int64_t dot_i32(const std::int16_t* w2,
                                          const std::int32_t* acc,
                                          std::size_t n) {
    std::int64_t sum = 0;
    for (std::size_t i = 0; i < n; ++i) {
        sum += static_cast<std::int64_t>(w2[i]) * acc[i];
    }
    return sum;
}

// The fused per-boundary Int16 score (the whole hidden-unit pass in one
// register-resident sweep, I.2). Semantically identical to, and bit-exact
// with, the sequential steps applied in this exact order:
//   acc = b1;  for each block: acc = sat16(acc + block);  acc = max(acc, 0);
//   return Σ w2·acc
// `blocks[0..nblocks)` are the H-length int16 contributions (the 2w window
// table blocks in window order, then any dictionary columns), added with
// per-lane saturation in that order. The fusion keeps each H-chunk of the
// accumulator in registers across all the adds and the relu, eliminating the
// ~2w redundant accumulator round-trips through memory that dominate the
// scoring hot path. Covers hidden units [begin, end); the SIMD kernels finish
// their tails here.
[[nodiscard]] inline std::int64_t fused_score_i16(
    const std::int16_t* b1, const std::int16_t* const* blocks,
    std::size_t nblocks, const std::int16_t* w2, std::size_t begin,
    std::size_t end) {
    std::int64_t sum = 0;
    for (std::size_t i = begin; i < end; ++i) {
        std::int32_t a = b1[i];
        for (std::size_t b = 0; b < nblocks; ++b) {
            a = std::clamp<std::int32_t>(a + blocks[b][i], -32768, 32767);
        }
        a = std::max<std::int32_t>(a, 0);  // relu
        sum += static_cast<std::int64_t>(w2[i]) * a;
    }
    return sum;
}

}  // namespace scalar

#if defined(SEGMENTLIB_KERNELS_NEON)

inline void add_i32(const std::int32_t* src, std::int32_t* acc, std::size_t n) {
    std::size_t i = 0;
    for (; i + 4 <= n; i += 4) {
        vst1q_s32(acc + i, vaddq_s32(vld1q_s32(acc + i), vld1q_s32(src + i)));
    }
    scalar::add_i32(src + i, acc + i, n - i);
}

inline void relu_i32(std::int32_t* acc, std::size_t n) {
    const int32x4_t zero = vdupq_n_s32(0);
    std::size_t i = 0;
    for (; i + 4 <= n; i += 4) {
        vst1q_s32(acc + i, vmaxq_s32(vld1q_s32(acc + i), zero));
    }
    scalar::relu_i32(acc + i, n - i);
}

[[nodiscard]] inline std::int64_t dot_i32(const std::int16_t* w2,
                                          const std::int32_t* acc,
                                          std::size_t n) {
    int64x2_t sum = vdupq_n_s64(0);
    std::size_t i = 0;
    for (; i + 4 <= n; i += 4) {
        const int32x4_t w = vmovl_s16(vld1_s16(w2 + i));  // widen int16→int32
        const int32x4_t a = vld1q_s32(acc + i);
        sum = vaddq_s64(sum, vmull_s32(vget_low_s32(w), vget_low_s32(a)));
        sum = vaddq_s64(sum, vmull_high_s32(w, a));
    }
    return vaddvq_s64(sum) + scalar::dot_i32(w2 + i, acc + i, n - i);
}

[[nodiscard]] inline std::int64_t fused_score_i16(
    const std::int16_t* b1, const std::int16_t* const* blocks,
    std::size_t nblocks, const std::int16_t* w2, std::size_t h) {
    const int16x8_t zero = vdupq_n_s16(0);
    int64x2_t sum = vdupq_n_s64(0);
    std::size_t i = 0;
    for (; i + 8 <= h; i += 8) {
        int16x8_t a = vld1q_s16(b1 + i);
        for (std::size_t b = 0; b < nblocks; ++b) {
            a = vqaddq_s16(a, vld1q_s16(blocks[b] + i));  // saturating, in order
        }
        a = vmaxq_s16(a, zero);  // relu
        const int16x8_t w = vld1q_s16(w2 + i);
        // int16×int16 → int32×4 per half, pairwise-accumulate into int64
        // (associative, so the grouping does not change the sum).
        sum = vpadalq_s32(sum, vmull_s16(vget_low_s16(w), vget_low_s16(a)));
        sum = vpadalq_s32(sum, vmull_high_s16(w, a));
    }
    std::int64_t total = vaddvq_s64(sum);
    return total + scalar::fused_score_i16(b1, blocks, nblocks, w2, i, h);
}

#elif defined(SEGMENTLIB_KERNELS_AVX2)

inline void add_i32(const std::int32_t* src, std::int32_t* acc, std::size_t n) {
    std::size_t i = 0;
    for (; i + 8 <= n; i += 8) {
        const __m256i a =
            _mm256_loadu_si256(reinterpret_cast<const __m256i*>(acc + i));
        const __m256i s =
            _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + i));
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(acc + i),
                            _mm256_add_epi32(a, s));
    }
    scalar::add_i32(src + i, acc + i, n - i);
}

inline void relu_i32(std::int32_t* acc, std::size_t n) {
    const __m256i zero = _mm256_setzero_si256();
    std::size_t i = 0;
    for (; i + 8 <= n; i += 8) {
        const __m256i a =
            _mm256_loadu_si256(reinterpret_cast<const __m256i*>(acc + i));
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(acc + i),
                            _mm256_max_epi32(a, zero));
    }
    scalar::relu_i32(acc + i, n - i);
}

[[nodiscard]] inline std::int64_t dot_i32(const std::int16_t* w2,
                                          const std::int32_t* acc,
                                          std::size_t n) {
    __m256i sum = _mm256_setzero_si256();  // 4 × int64 lanes
    std::size_t i = 0;
    for (; i + 8 <= n; i += 8) {
        // Widen both operands to sign-extended 64-bit lanes; mul_epi32
        // multiplies the (sign-extended) low 32 bits of each lane — exact.
        const __m256i w32 = _mm256_cvtepi16_epi32(
            _mm_loadu_si128(reinterpret_cast<const __m128i*>(w2 + i)));
        const __m256i a32 =
            _mm256_loadu_si256(reinterpret_cast<const __m256i*>(acc + i));
        const __m256i w_lo = _mm256_cvtepi32_epi64(_mm256_castsi256_si128(w32));
        const __m256i w_hi =
            _mm256_cvtepi32_epi64(_mm256_extracti128_si256(w32, 1));
        const __m256i a_lo = _mm256_cvtepi32_epi64(_mm256_castsi256_si128(a32));
        const __m256i a_hi =
            _mm256_cvtepi32_epi64(_mm256_extracti128_si256(a32, 1));
        sum = _mm256_add_epi64(sum, _mm256_mul_epi32(w_lo, a_lo));
        sum = _mm256_add_epi64(sum, _mm256_mul_epi32(w_hi, a_hi));
    }
    alignas(32) std::int64_t lanes[4];
    _mm256_store_si256(reinterpret_cast<__m256i*>(lanes), sum);
    return lanes[0] + lanes[1] + lanes[2] + lanes[3] +
           scalar::dot_i32(w2 + i, acc + i, n - i);
}

[[nodiscard]] inline std::int64_t fused_score_i16(
    const std::int16_t* b1, const std::int16_t* const* blocks,
    std::size_t nblocks, const std::int16_t* w2, std::size_t h) {
    const __m256i zero = _mm256_setzero_si256();
    __m256i sum = _mm256_setzero_si256();  // 4 × int64 lanes
    std::size_t i = 0;
    for (; i + 16 <= h; i += 16) {
        __m256i a = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(b1 + i));
        for (std::size_t b = 0; b < nblocks; ++b) {
            a = _mm256_adds_epi16(  // saturating, in order
                a, _mm256_loadu_si256(
                       reinterpret_cast<const __m256i*>(blocks[b] + i)));
        }
        a = _mm256_max_epi16(a, zero);  // relu
        const __m256i w =
            _mm256_loadu_si256(reinterpret_cast<const __m256i*>(w2 + i));
        // madd: int16×int16 pair sums → 8 × int32; each ≤ 2·32767² < 2^31 (a is
        // relu'd into [0,32767]), so the pair sum cannot overflow int32.
        const __m256i pairs = _mm256_madd_epi16(w, a);
        sum = _mm256_add_epi64(
            sum, _mm256_cvtepi32_epi64(_mm256_castsi256_si128(pairs)));
        sum = _mm256_add_epi64(
            sum, _mm256_cvtepi32_epi64(_mm256_extracti128_si256(pairs, 1)));
    }
    alignas(32) std::int64_t lanes[4];
    _mm256_store_si256(reinterpret_cast<__m256i*>(lanes), sum);
    std::int64_t total = lanes[0] + lanes[1] + lanes[2] + lanes[3];
    return total + scalar::fused_score_i16(b1, blocks, nblocks, w2, i, h);
}

#else  // scalar dispatch

inline void add_i32(const std::int32_t* src, std::int32_t* acc, std::size_t n) {
    scalar::add_i32(src, acc, n);
}
inline void relu_i32(std::int32_t* acc, std::size_t n) {
    scalar::relu_i32(acc, n);
}
[[nodiscard]] inline std::int64_t dot_i32(const std::int16_t* w2,
                                          const std::int32_t* acc,
                                          std::size_t n) {
    return scalar::dot_i32(w2, acc, n);
}
[[nodiscard]] inline std::int64_t fused_score_i16(
    const std::int16_t* b1, const std::int16_t* const* blocks,
    std::size_t nblocks, const std::int16_t* w2, std::size_t h) {
    return scalar::fused_score_i16(b1, blocks, nblocks, w2, 0, h);
}

#endif

}  // namespace segmentlib::mlp::kernels
