//
//  succinct.h
//
//  Copyright (c) 2026 Yuji Hirose. All rights reserved.
//  MIT License
//

#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#if defined(_MSC_VER)
#include <intrin.h>
#endif

// Succinct-data-structure primitives for the Elias-Fano compressed postings
// (see docs/postings_compression_design.ja.md). Internal building blocks --
// not part of the public searchlib.h API.

namespace searchlib {
namespace detail {

inline size_t popcount64(uint64_t x) {
#if defined(_MSC_VER)
  return static_cast<size_t>(__popcnt64(x));
#else
  return static_cast<size_t>(__builtin_popcountll(x));
#endif
}

inline size_t ctz64(uint64_t x) {
#if defined(_MSC_VER)
  unsigned long index;
  _BitScanForward64(&index, x);
  return static_cast<size_t>(index);
#else
  return static_cast<size_t>(__builtin_ctzll(x));
#endif
}

// Position of the k-th (0-based) set bit within a word. k must be less than
// popcount64(x).
inline size_t select_in_word(uint64_t x, size_t k) {
  for (size_t i = 0; i < k; i++) {
    x &= x - 1; // clear the lowest set bit
  }
  return ctz64(x);
}

// A plain bit vector with O(1) rank and O(log(n/512)) select. Constructed by
// set()-ing bits, then build() finalizes the rank index; the vector is
// immutable (and therefore safe to share across reader threads) afterwards.
//
// The rank index is one cumulative uint64_t per 512-bit superblock, so a
// rank is one table lookup plus at most 8 popcounts. Selects binary-search
// the superblock table and then scan within one superblock; at the scale
// this library targets that is a handful of steps and profiling should come
// before any fancier select structure.
class BitVector {
public:
  BitVector() = default;

  explicit BitVector(size_t bit_count)
      : bit_count_(bit_count), words_((bit_count + 63) / 64, 0) {}

  size_t size() const { return bit_count_; }

  void set(size_t i) { words_[i / 64] |= uint64_t(1) << (i % 64); }

  bool get(size_t i) const { return (words_[i / 64] >> (i % 64)) & 1; }

  // Builds the rank index. Must be called after the last set() and before
  // any rank/select query.
  void build() {
    auto superblock_count = superblock_count_();
    rank_.assign(superblock_count + 1, 0);
    uint64_t count = 0;
    for (size_t w = 0; w < words_.size(); w++) {
      if (w % kWordsPerSuperblock == 0) {
        rank_[w / kWordsPerSuperblock] = count;
      }
      count += popcount64(words_[w]);
    }
    rank_[superblock_count] = count;
    ones_ = static_cast<size_t>(count);
  }

  size_t ones() const { return ones_; }
  size_t zeros() const { return bit_count_ - ones_; }

  // Number of 1 bits in [0, i). i may equal size().
  size_t rank1(size_t i) const {
    auto word_index = i / 64;
    auto count = static_cast<size_t>(rank_[i / kSuperblockBits]);
    for (auto w = (i / kSuperblockBits) * kWordsPerSuperblock; w < word_index;
         w++) {
      count += popcount64(words_[w]);
    }
    auto rem = i % 64;
    if (rem > 0) {
      count += popcount64(words_[word_index] & ((uint64_t(1) << rem) - 1));
    }
    return count;
  }

  // Number of 0 bits in [0, i). i may equal size().
  size_t rank0(size_t i) const { return i - rank1(i); }

  // Position of the k-th (0-based) 1 bit. k must be less than ones().
  size_t select1(size_t k) const {
    // Superblock containing the (k+1)-th one: largest s with rank_[s] <= k.
    auto sb = superblock_search_(
        [&](size_t s) { return static_cast<size_t>(rank_[s]); }, k);

    auto remaining = k - static_cast<size_t>(rank_[sb]);
    for (auto w = sb * kWordsPerSuperblock; w < words_.size(); w++) {
      auto count = popcount64(words_[w]);
      if (remaining < count) {
        return w * 64 + select_in_word(words_[w], remaining);
      }
      remaining -= count;
    }
    return bit_count_; // unreachable when k < ones()
  }

  // Position of the k-th (0-based) 0 bit. k must be less than zeros().
  size_t select0(size_t k) const {
    // Zeros before superblock s. The final superblock boundary may lie past
    // bit_count_; clamp so that the unused tail of the last word (always
    // zero-filled but outside the logical bit range) is not counted.
    auto zeros_at = [&](size_t s) {
      auto boundary = s * kSuperblockBits;
      if (boundary > bit_count_) {
        boundary = bit_count_;
      }
      return boundary - static_cast<size_t>(rank_[s]);
    };
    auto sb = superblock_search_(zeros_at, k);

    auto remaining = k - zeros_at(sb);
    for (auto w = sb * kWordsPerSuperblock; w < words_.size(); w++) {
      auto valid = bit_count_ - w * 64;
      if (valid > 64) {
        valid = 64;
      }
      auto count = valid - popcount64(words_[w]);
      if (remaining < count) {
        return w * 64 + select_in_word(~words_[w], remaining);
      }
      remaining -= count;
    }
    return bit_count_; // unreachable when k < zeros()
  }

private:
  static constexpr size_t kSuperblockBits = 512;
  static constexpr size_t kWordsPerSuperblock = kSuperblockBits / 64;

  size_t superblock_count_() const {
    return (words_.size() + kWordsPerSuperblock - 1) / kWordsPerSuperblock;
  }

  // Largest superblock s in [0, superblock_count) whose cumulative count
  // (per `cumulative`) is <= k. Relies on the sentinel entry at
  // superblock_count being > k.
  template <typename Cumulative>
  size_t superblock_search_(Cumulative cumulative, size_t k) const {
    size_t low = 1;
    auto high = superblock_count_();
    while (low < high) {
      auto mid = low + (high - low) / 2;
      if (cumulative(mid) > k) {
        high = mid;
      } else {
        low = mid + 1;
      }
    }
    return low - 1;
  }

  size_t bit_count_ = 0;
  size_t ones_ = 0;
  std::vector<uint64_t> words_;
  std::vector<uint64_t> rank_;
};

} // namespace detail
} // namespace searchlib
