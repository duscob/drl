//
// Created by Dustin Cobas Batista <dustin.cobas@gmail.com> on 6/2/2018.
//


#include <gtest/gtest.h>

#include <sdsl/csa_wt.hpp>
#include <sdsl/csa_bitcompressed.hpp>
#include <sdsl/suffix_array_algorithm.hpp>

#include <grammar/slp.h>
#include <grammar/slp_metadata.h>

#include "drl/pdloda.h"


class PDLODASearch_TF : public ::testing::TestWithParam<std::tuple<std::string, std::vector<uint32_t>>> {
 public:
  static std::string tmp_filename;

  static void SetUpTestCase() {
    tmp_filename = std::tmpnam(nullptr);

    std::ofstream out(tmp_filename, std::ios::binary);
    out << "TATA$LATA$AAAA$";
  }

  static void TearDownTestCase() {
    std::remove(tmp_filename.c_str());
  }
};


std::string PDLODASearch_TF::tmp_filename;


TEST_P(PDLODASearch_TF, search) {
  drl::PDLODA<sdsl::csa_wt<>, grammar::SLP<>, grammar::PTS<>, drl::ComputeSpanCoverFromTopWrapper> idx(
      tmp_filename,
      '$',
      drl::ConstructSLPAndComputePTS());

  const auto &pattern = std::get<0>(GetParam());
  const auto &eresult = std::get<1>(GetParam());

  std::vector<uint32_t> result;
  idx.Search(pattern.begin(), pattern.end(), std::back_inserter(result));
  EXPECT_EQ(result, eresult);
}


INSTANTIATE_TEST_CASE_P(
    PDLODA,
    PDLODASearch_TF,
    ::testing::Values(
        std::make_tuple("TA", std::vector<uint32_t>{1, 2}),
        std::make_tuple("A", std::vector<uint32_t>{1, 2, 3}),
        std::make_tuple("LT", std::vector<uint32_t>{}),
        std::make_tuple("AA", std::vector<uint32_t>{3}),
        std::make_tuple("TATA", std::vector<uint32_t>{1})
    )
);


template<typename T>
class PDLODAGeneric_TF : public ::testing::Test {
 public:
  static std::string tmp_filename;

  static void SetUpTestCase() {
    tmp_filename = std::tmpnam(nullptr);

    std::ofstream out(tmp_filename, std::ios::binary);
    out << "TATA$LATA$AAAA$";
  }

  static void TearDownTestCase() {
    std::remove(tmp_filename.c_str());
  }
};


template<typename T>
std::string PDLODAGeneric_TF<T>::tmp_filename;

using MyTypes = ::testing::Types<
    std::pair<drl::PDLODA<sdsl::csa_wt<>, grammar::SLP<>, grammar::PTS<>, drl::ComputeSpanCoverFromTopWrapper>,
              drl::ConstructSLPAndComputePTS>,
    std::pair<drl::PDLODA<sdsl::csa_bitcompressed<>,
                          grammar::SLP<>,
                          grammar::PTS<>,
                          drl::ComputeSpanCoverFromTopWrapper>,
              drl::ConstructSLPAndComputePTS>,
    std::pair<drl::PDLODA<sdsl::csa_bitcompressed<>,
                          grammar::SLP<>,
                          grammar::SampledPTS<grammar::SLP<>>,
                          drl::ComputeSpanCoverFromTopWrapper>,
              drl::ConstructSLPAndComputePTS>,
    std::pair<drl::PDLODA<sdsl::csa_bitcompressed<>,
                          grammar::SampledSLP<>,
                          grammar::Chunks<>,
                          drl::ComputeSpanCoverFromBottomWrapper>,
              drl::ComputeSLPAndPTS>>;
TYPED_TEST_CASE(PDLODAGeneric_TF, MyTypes);


TYPED_TEST(PDLODAGeneric_TF, search) {
  typename TypeParam::first_type idx(this->tmp_filename, '$', typename TypeParam::second_type());

  std::string pattern = "TA";
  std::vector<uint32_t> eresult = {1, 2};

  std::vector<uint32_t> result;
  idx.Search(pattern.begin(), pattern.end(), std::back_inserter(result));
  EXPECT_EQ(result, eresult);
}
