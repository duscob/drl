//
// Created by Dustin Cobas <dustin.cobas@gmail.com> on 6/2/2018.
//

#include <cstdio>

#include <gtest/gtest.h>

#include <sdsl/int_vector.hpp>

#include "drl/construct_da.h"


class ConstructDocBorderTest : public ::testing::TestWithParam<std::tuple<const char *, char, sdsl::bit_vector>> {
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


TEST_P(ConstructDocBorderTest, construct) {
  sdsl::bit_vector doc_border;

  drl::ConstructDocBorder(tmp_filename, doc_border, std::get<1>(GetParam()));
  EXPECT_EQ(doc_border, std::get<2>(GetParam()));
}


INSTANTIATE_TEST_CASE_P(InstantiationName,
                        ConstructDocBorderTest,
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
                                            sdsl::bit_vector())));


int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}