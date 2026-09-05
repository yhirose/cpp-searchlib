#include <gtest/gtest.h>

#include <algorithm>
#include <random>
#include <vector>

#include <searchlib.h>

using searchlib::detail::BitVector;
using searchlib::detail::EliasFano;

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

// Cross-checks every access() against the original values and next_geq()
// against std::lower_bound, probing each value itself, its neighbors, and
// the universe boundaries.
static void verify_elias_fano(const std::vector<uint64_t> &values,
                              uint64_t universe) {
  EliasFano ef(values, universe);

  ASSERT_EQ(values.size(), ef.size());
  for (size_t i = 0; i < values.size(); i++) {
    ASSERT_EQ(values[i], ef.access(i)) << "access(" << i << ")";
  }

  // read() must answer exactly what a run of access() calls would, from every
  // start and in every block size -- it reaches the values by a different
  // route (one select, then a forward scan) than access() does.
  for (size_t block : {size_t(1), size_t(2), size_t(7), size_t(64),
                       size_t(1000)}) {
    std::vector<uint64_t> out(block + 1, ~uint64_t(0));
    for (size_t start = 0; start <= values.size(); start++) {
      auto written = ef.read(start, out.data(), block);
      ASSERT_EQ(std::min(block, values.size() - start), written)
          << "read(" << start << ", " << block << ")";
      for (size_t i = 0; i < written; i++) {
        ASSERT_EQ(values[start + i], out[i])
            << "read(" << start << ", " << block << ")[" << i << "]";
      }
      // Past the end is empty, never a partial write.
      ASSERT_EQ(~uint64_t(0), out[block]);
    }
    ASSERT_EQ(0u, ef.read(values.size() + 5, out.data(), block));
  }

  auto expect_next_geq = [&](uint64_t target) {
    auto expected = static_cast<size_t>(
        std::lower_bound(values.begin(), values.end(), target) -
        values.begin());
    ASSERT_EQ(expected, ef.next_geq(target)) << "next_geq(" << target << ")";
  };

  expect_next_geq(0);
  if (universe > 0) {
    expect_next_geq(universe - 1);
  }
  for (auto value : values) {
    expect_next_geq(value);
    if (value > 0) {
      expect_next_geq(value - 1);
    }
    if (value + 1 < universe) {
      expect_next_geq(value + 1);
    }
  }
}

TEST(EliasFanoTest, EmptyAndTiny) {
  verify_elias_fano({}, 0);
  verify_elias_fano({}, 100);
  verify_elias_fano({0}, 1);
  verify_elias_fano({42}, 100);
  verify_elias_fano({0, 1, 2, 3}, 4); // fully dense, low_bits = 0
}

TEST(EliasFanoTest, Duplicates) {
  verify_elias_fano({5, 5, 5, 9, 9, 100}, 200);
}

TEST(EliasFanoTest, HugeUniverse) {
  // Sparse values over a 2^40 universe exercise wide low-bit widths.
  verify_elias_fano({0, uint64_t(1) << 20, uint64_t(1) << 39},
                    uint64_t(1) << 40);
  // Single element with a near-2^63 universe guards the low-bit width cap.
  verify_elias_fano({123}, uint64_t(1) << 63);
}

TEST(EliasFanoTest, Randomized) {
  std::mt19937_64 rng(42); // fixed seed for reproducibility
  // Universe multipliers cover dense (~n), medium, and sparse (~1000n).
  for (uint64_t multiplier : {1u, 3u, 37u, 1000u}) {
    for (size_t n : {1u, 2u, 100u, 5000u}) {
      auto universe = static_cast<uint64_t>(n) * multiplier + 1;
      std::uniform_int_distribution<uint64_t> pick(0, universe - 1);
      std::vector<uint64_t> values(n);
      for (auto &value : values) {
        value = pick(rng);
      }
      std::sort(values.begin(), values.end()); // duplicates possible and fine
      verify_elias_fano(values, universe);

      // Also probe random targets, not just neighborhoods of members.
      EliasFano ef(values, universe);
      for (int probe = 0; probe < 100; probe++) {
        auto target = pick(rng);
        auto expected = static_cast<size_t>(
            std::lower_bound(values.begin(), values.end(), target) -
            values.begin());
        ASSERT_EQ(expected, ef.next_geq(target));
      }
    }
  }
}
