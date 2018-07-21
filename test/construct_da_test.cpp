//
// Created by Dustin Cobas <dustin.cobas@gmail.com> on 6/2/2018.
//

#include <cstdio>

#include <gtest/gtest.h>

#include <sdsl/int_vector.hpp>
#include <sdsl/rank_support_v.hpp>
#include <sdsl/suffix_arrays.hpp>

#include "drl/construct_da.h"


class DocBorderTest : public ::testing::TestWithParam<std::tuple<const char *, char, sdsl::bit_vector>> {
 protected:
  std::string tmp_filename;

  void SetUp() override {
    tmp_filename = std::tmpnam(nullptr);

    std::ofstream out(tmp_filename, std::ios::binary);
    out << std::get<0>(GetParam());
  }

  void TearDown() override {
    std::remove(tmp_filename.c_str());
  }
};


TEST_P(DocBorderTest, construct_with_iterators) {
  auto str = std::get<0>(GetParam());
  auto size = std::strlen(str);
  auto doc_delim = std::get<1>(GetParam());

  sdsl::bit_vector doc_border(size);

  drl::ConstructDocBorderBasic(str, str + size, doc_border.begin(), doc_delim);
  EXPECT_EQ(doc_border, std::get<2>(GetParam()));
}


TEST_P(DocBorderTest, construct_with_file_iterators) {
  auto str = std::get<0>(GetParam());
  auto size = std::strlen(str);
  auto doc_delim = std::get<1>(GetParam());
  std::ifstream in(this->tmp_filename, std::ios::binary);

  sdsl::bit_vector doc_border(size);

  drl::ConstructDocBorderBasic(std::istream_iterator<int8_t>(in),
                               std::istream_iterator<int8_t>{},
                               doc_border.begin(),
                               doc_delim);
  EXPECT_EQ(doc_border, std::get<2>(GetParam()));
}


TEST_P(DocBorderTest, construct_with_iterators_and_container) {
  const std::string &str = std::get<0>(GetParam());
  auto doc_delim = std::get<1>(GetParam());

  sdsl::bit_vector doc_border;

  drl::ConstructDocBorder(str.begin(), str.end(), doc_border, doc_delim);
  EXPECT_EQ(doc_border, std::get<2>(GetParam()));
}


TEST_P(DocBorderTest, construct_with_file_iterators_and_container) {
  const std::string &str = std::get<0>(GetParam());
  auto doc_delim = std::get<1>(GetParam());

  sdsl::bit_vector doc_border;

  std::ifstream in(this->tmp_filename, std::ios::binary);
  drl::ConstructDocBorder(std::istream_iterator<int8_t>(in),
                          std::istream_iterator<int8_t>{},
                          doc_border,
                          doc_delim,
                          str.size());
  EXPECT_EQ(doc_border, std::get<2>(GetParam()));
}


TEST_P(DocBorderTest, construct_file) {
  auto doc_delim = std::get<1>(GetParam());

  sdsl::bit_vector doc_border;

  drl::ConstructDocBorder(tmp_filename, doc_border, doc_delim);
  EXPECT_EQ(doc_border, std::get<2>(GetParam()));
}


INSTANTIATE_TEST_CASE_P(
    DocBorder,
    DocBorderTest,
    ::testing::Values(
        std::make_tuple("TATA$LATA$AAAA$",
                        '$',
                        sdsl::bit_vector({0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1})),
        std::make_tuple("TATA$LATA$AAAA$",
                        'T',
                        sdsl::bit_vector({1, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0})),
        std::make_tuple("TATA$LATA$AAAA$",
                        'X',
                        sdsl::bit_vector({0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})),
        std::make_tuple("",
                        'X',
                        sdsl::bit_vector())
    )
);


class DocArrayTest : public ::testing::TestWithParam<std::tuple<const char *, char, sdsl::int_vector<>>> {
 protected:
  std::string tmp_filename;

  void SetUp() override {
    tmp_filename = std::tmpnam(nullptr);

    std::ofstream out(tmp_filename, std::ios::binary);
    out << std::get<0>(GetParam());
  }

  void TearDown() override {
    std::remove(tmp_filename.c_str());
  }
};


TEST_P(DocArrayTest, construct) {
  auto doc_delim = std::get<1>(GetParam());

  sdsl::csa_bitcompressed<> csa;
  construct(csa, tmp_filename, 1);

  sdsl::bit_vector doc_border;
//  drl::ConstructDocBorder(tmp_filename, doc_border, doc_delim);
  std::ifstream in(this->tmp_filename, std::ios::binary);
  drl::ConstructDocBorder(std::istream_iterator<int8_t>(in),
                          std::istream_iterator<int8_t>{},
                          doc_border,
                          doc_delim,
                          csa.size());
  sdsl::bit_vector::rank_1_type b_rank(&doc_border);

  sdsl::int_vector<> doc_array;
  drl::ConstructDocArray(csa, b_rank, doc_array);
  EXPECT_EQ(doc_array, std::get<2>(GetParam()));
}


INSTANTIATE_TEST_CASE_P(
    DocArray,
    DocArrayTest,
    ::testing::Values(
        std::make_tuple("TATA$LATA$AAAA$",
                        '$',
                        sdsl::int_vector<>({3, 2, 1, 0, 2, 1, 0, 2, 2, 2, 1, 0, 1, 1, 0, 0})),
        std::make_tuple("TATA$LATA$AAAA$",
                        'X',
                        sdsl::int_vector<>({0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}))
    )
);
