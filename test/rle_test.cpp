//
// Created by Dustin Cobas Batista <dustin.cobas@gmail.com> on 2019-08-21.
//

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <sdsl/bit_vectors.hpp>

#include "drl/rle.h"

class RLETF : public testing::TestWithParam<std::tuple<std::vector<int>, std::vector<bool>, std::vector<std::size_t>>> {
};

TEST_P(RLETF, Build) {
  const auto &seq = std::get<0>(GetParam());

  sdsl::bit_vector bv_runs;
  drl::BuildRLEncoding(begin(seq), end(seq), bv_runs);

  const auto &e_bv_runs = std::get<1>(GetParam());
  EXPECT_THAT(bv_runs, ::testing::ElementsAreArray(e_bv_runs));
}

TEST_P(RLETF, Position) {
  const auto &seq = std::get<0>(GetParam());

  sdsl::bit_vector tmp_bv_runs;
  drl::BuildRLEncoding(begin(seq), end(seq), tmp_bv_runs);
  drl::RLEncoding<sdsl::sd_vector<>> rle(tmp_bv_runs);

  const auto &e_pos = std::get<2>(GetParam());
  for (std::size_t i = 0; i < seq.size(); ++i) {
    EXPECT_EQ(rle(i), e_pos[i]) << "doesn't match at position " << i;
  }
}

INSTANTIATE_TEST_SUITE_P(RLE,
                         RLETF,
                         ::testing::Values(
                             std::make_tuple(std::vector<int>{1, 2, 3, 4, 5},
                                             std::vector<bool>{0, 0, 0, 0, 0},
                                             std::vector<std::size_t>{0, 1, 2, 3, 4}),
                             std::make_tuple(std::vector<int>{5, 5, 5, 5, 5},
                                             std::vector<bool>{0, 1, 1, 1, 1},
                                             std::vector<std::size_t>{0, 0, 0, 0, 0}),
                             std::make_tuple(std::vector<int>{5, 5, 5, 5, 5, 3},
                                             std::vector<bool>{0, 1, 1, 1, 1, 0},
                                             std::vector<std::size_t>{0, 0, 0, 0, 0, 1}),
                             std::make_tuple(std::vector<int>{4, 3, 2, 1, 3, 2, 1, 3, 3, 3, 2, 1, 2, 2, 1, 1},
                                             std::vector<bool>{0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 1, 0, 1},
                                             std::vector<std::size_t>{0, 1, 2, 3, 4, 5, 6, 7, 7, 7, 8, 9, 10, 10, 11,
                                                                      11})
                         )
);

class RLEIteratorTF : public testing::TestWithParam<std::tuple<std::vector<int>, std::vector<int>>> {
};

TEST_P(RLEIteratorTF, PreIncrement) {
  const auto &seq = std::get<0>(GetParam());
  const auto &rle_seq = std::get<1>(GetParam());

  drl::RLEIterator<decltype(seq.begin())> iter(seq.begin(), seq.end());
  drl::RLEIterator<decltype(seq.begin())> eiter(seq.end(), seq.end());

  EXPECT_THAT(rle_seq, ::testing::ElementsAreArray(iter, eiter));
}

TEST_P(RLEIteratorTF, PostIncrement) {
  const auto &seq = std::get<0>(GetParam());
  const auto &rle_seq = std::get<1>(GetParam());

  drl::RLEIterator<decltype(seq.begin())> iter(seq.begin(), seq.end());
  drl::RLEIterator<decltype(seq.begin())> eiter(seq.end(), seq.end());

  ASSERT_EQ(distance(iter, eiter), rle_seq.size());
  for (auto it1 = iter, it2 = iter; it1 != eiter; ++it1) {
    EXPECT_EQ(it1, it2++);
  }
}

INSTANTIATE_TEST_SUITE_P(RLE,
                         RLEIteratorTF,
                         ::testing::Values(
                             std::make_tuple(std::vector<int>{1, 2, 3, 4, 5},
                                             std::vector<int>{1, 2, 3, 4, 5}),
                             std::make_tuple(std::vector<int>{5, 5, 5, 5, 5},
                                             std::vector<int>{5}),
                             std::make_tuple(std::vector<int>{5, 5, 5, 5, 5, 3},
                                             std::vector<int>{5, 3}),
                             std::make_tuple(std::vector<int>{4, 3, 2, 1, 3, 2, 1, 3, 3, 3, 2, 1, 2, 2, 1, 1},
                                             std::vector<int>{4, 3, 2, 1, 3, 2, 1, 3, 2, 1, 2, 1})
                         )
);