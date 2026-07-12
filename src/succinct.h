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

// Elias-Fano encoding of a monotone (non-decreasing) uint64_t sequence,
// using close to the information-theoretic minimum n*(2 + log2(U/n)) bits.
// Each value is split into `low_bits_` low bits (packed fixed-width) and the
// remaining high bits (unary-coded bucket sizes in a BitVector), so that
// access(i) is one select1 and next_geq(target) is two select0 plus a scan
// of one bucket (~2 entries on average). Immutable after construction.
class EliasFano {
public:
  EliasFano() = default;

  // values must be sorted (duplicates allowed) with every element less than
  // universe.
  EliasFano(const std::vector<uint64_t> &values, uint64_t universe)
      : size_(values.size()), universe_(universe) {
    if (size_ == 0) {
      return;
    }

    // Low-bit width l = floor(log2(U/n)) keeps the high-bits vector at most
    // ~2n + 1 bits long (bucket count <= 2n).
    auto ratio = universe_ / size_;
    while (low_bits_ + 1 < 64 && (uint64_t(1) << (low_bits_ + 1)) <= ratio) {
      low_bits_++;
    }

    auto bucket_count = static_cast<size_t>((universe_ - 1) >> low_bits_) + 1;
    high_ = BitVector(size_ + bucket_count);
    low_words_.assign((size_ * low_bits_ + 63) / 64 + 1, 0);

    for (size_t i = 0; i < size_; i++) {
      auto value = values[i];
      high_.set(static_cast<size_t>(value >> low_bits_) + i);
      if (low_bits_ > 0) {
        auto bit = i * low_bits_;
        auto low = value & low_mask_();
        low_words_[bit / 64] |= low << (bit % 64);
        if (bit % 64 + low_bits_ > 64) {
          low_words_[bit / 64 + 1] |= low >> (64 - bit % 64);
        }
      }
    }
    high_.build();
  }

  size_t size() const { return size_; }
  uint64_t universe() const { return universe_; }

  // The i-th value. i must be less than size().
  uint64_t access(size_t i) const {
    auto high = static_cast<uint64_t>(high_.select1(i) - i);
    return (high << low_bits_) | low_(i);
  }

  // Index of the first value >= target, or size() if none. The bucket that
  // could contain target is located with two select0 calls, then scanned
  // linearly.
  size_t next_geq(uint64_t target) const {
    if (size_ == 0 || target >= universe_) {
      return target >= universe_ ? size_ : 0;
    }

    auto bucket = static_cast<size_t>(target >> low_bits_);
    // Ones before a bucket = position of its preceding zero terminator
    // minus the number of zeros before that terminator.
    auto begin =
        bucket == 0 ? 0 : high_.select0(bucket - 1) - (bucket - 1);
    auto end = high_.select0(bucket) - bucket;

    auto low_target = target & low_mask_();
    for (auto i = begin; i < end; i++) {
      if (low_(i) >= low_target) {
        return i;
      }
    }
    // The first value of any later bucket has a larger high part, so `end`
    // (== size() when the tail is empty) is the answer.
    return end;
  }

private:
  uint64_t low_mask_() const {
    return low_bits_ == 0 ? 0 : (uint64_t(1) << low_bits_) - 1;
  }

  uint64_t low_(size_t i) const {
    if (low_bits_ == 0) {
      return 0;
    }
    auto bit = i * low_bits_;
    auto value = low_words_[bit / 64] >> (bit % 64);
    if (bit % 64 + low_bits_ > 64) {
      value |= low_words_[bit / 64 + 1] << (64 - bit % 64);
    }
    return value & low_mask_();
  }

  size_t size_ = 0;
  uint64_t universe_ = 0;
  size_t low_bits_ = 0;
  BitVector high_;
  std::vector<uint64_t> low_words_;
};

} // namespace detail
} // namespace searchlib
