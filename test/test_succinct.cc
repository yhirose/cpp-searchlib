#include <gtest/gtest.h>

#include <random>
#include <vector>

#include "succinct.h"

using searchlib::detail::BitVector;

// Builds a BitVector from a plain bool vector and cross-checks every rank
// and select answer against naive prefix sums / position lists.
static void verify_against_naive(const std::vector<bool> &bits) {
  BitVector bv(bits.size());
  std::vector<size_t> one_positions;
  std::vector<size_t> zero_positions;
  for (size_t i = 0; i < bits.size(); i++) {
    if (bits[i]) {
      bv.set(i);
      one_positions.push_back(i);
    } else {
      zero_positions.push_back(i);
    }
  }
  bv.build();

  ASSERT_EQ(bits.size(), bv.size());
  ASSERT_EQ(one_positions.size(), bv.ones());
  ASSERT_EQ(zero_positions.size(), bv.zeros());

  size_t ones_so_far = 0;
  for (size_t i = 0; i <= bits.size(); i++) {
    ASSERT_EQ(ones_so_far, bv.rank1(i)) << "rank1(" << i << ")";
    ASSERT_EQ(i - ones_so_far, bv.rank0(i)) << "rank0(" << i << ")";
    if (i < bits.size()) {
      ASSERT_EQ(bits[i], bv.get(i)) << "get(" << i << ")";
      if (bits[i]) {
        ones_so_far++;
      }
    }
  }

  for (size_t k = 0; k < one_positions.size(); k++) {
    ASSERT_EQ(one_positions[k], bv.select1(k)) << "select1(" << k << ")";
  }
  for (size_t k = 0; k < zero_positions.size(); k++) {
    ASSERT_EQ(zero_positions[k], bv.select0(k)) << "select0(" << k << ")";
  }
}

TEST(BitVectorTest, EmptyAndTiny) {
  verify_against_naive({});
  verify_against_naive({false});
  verify_against_naive({true});
  verify_against_naive({true, false, true});
}

TEST(BitVectorTest, AllZerosAllOnes) {
  for (size_t n : {1u, 63u, 64u, 65u, 511u, 512u, 513u, 1000u}) {
    verify_against_naive(std::vector<bool>(n, false));
    verify_against_naive(std::vector<bool>(n, true));
  }
}

TEST(BitVectorTest, WordAndSuperblockBoundaries) {
  // Alternating patterns across the 64-bit word and 512-bit superblock
  // boundaries that the rank/select index is built around.
  for (size_t n : {63u, 64u, 65u, 127u, 128u, 511u, 512u, 513u, 1024u}) {
    std::vector<bool> alternating(n);
    for (size_t i = 0; i < n; i++) {
      alternating[i] = (i % 2 == 0);
    }
    verify_against_naive(alternating);

    // A single bit set at the last position exercises the final partial word.
    std::vector<bool> last_only(n, false);
    last_only[n - 1] = true;
    verify_against_naive(last_only);
  }
}

TEST(BitVectorTest, Randomized) {
  std::mt19937 rng(42); // fixed seed for reproducibility
  for (double density : {0.01, 0.5, 0.99}) {
    std::bernoulli_distribution flip(density);
    for (size_t n : {65u, 513u, 4096u, 20000u}) {
      std::vector<bool> bits(n);
      for (size_t i = 0; i < n; i++) {
        bits[i] = flip(rng);
      }
      verify_against_naive(bits);
    }
  }
}
