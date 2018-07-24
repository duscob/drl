//
// Created by Dustin Cobas Batista <dustin.cobas@gmail.com> on 7/20/18.
//

#ifndef DRL_PDLODA_H
#define DRL_PDLODA_H

#include <vector>

#include <grammar/re_pair.h>
#include <grammar/slp_helper.h>

#include "construct_da.h"


namespace drl {

//! Precomputed Document List On Document Array
/**
 *
 * @tparam _SA Suffix Array
 * @tparam _SLP Straight-Line Program
 */
template<typename _SA, typename _SLP>
class PDLODA {
 public:
  template<typename _E = int8_t>
  PDLODA(const std::string &filename, const _E &doc_delim) {
    uint8_t num_bytes = sizeof(_E);
    std::string id = sdsl::util::basename(filename) + "_";
    sdsl::cache_config cconfig{false, "./", id};
    construct(sa_, filename, cconfig, num_bytes);

    {
      sdsl::bit_vector doc_border;

      {
        std::ifstream in(filename, std::ios::binary);
        drl::ConstructDocBorder(std::istream_iterator<_E>(in),
                                std::istream_iterator<_E>{},
                                doc_border,
                                doc_delim,
                                sa_.size());
      }
      sdsl::bit_vector::rank_1_type b_rank(&doc_border);

      sdsl::int_vector<> doc_array(sa_.size() + 1);
      drl::ConstructDocArray(sa_, b_rank, doc_array);

      grammar::RePairEncoder<true> encoder;
      ComputeSLP(doc_array.begin(), doc_array.end(), encoder, slp_);
    }
  }

  template<typename _OI>
  void SearchInRange(std::size_t begin, std::size_t end, _OI out) {
    std::vector<std::size_t> span_cover;
    ComputeSpanCover(slp_, begin, end, back_inserter(span_cover));

    if (span_cover.empty())
      return;

    std::vector<typename decltype(slp_.GetData(span_cover[0]))::value_type> docs;
    decltype(docs) tmp_docs;
//    for (int i = 0; i < span_cover.size(); ++i) {
//      tmp_docs.swap(docs);
//      docs.clear();
//      const auto &partial_docs = slp_.GetData(span_cover[i]);
//      set_union(tmp_docs.begin(), tmp_docs.end(), partial_docs.begin(), partial_docs.end(), back_inserter(docs));
//    }
    for (int i = 0; i < span_cover.size(); ++i) {
      const auto &partial_docs = slp_.GetData(span_cover[i]);

      tmp_docs.resize(docs.size() + partial_docs.size());
      tmp_docs.swap(docs);

      auto it = set_union(tmp_docs.begin(), tmp_docs.end(), partial_docs.begin(), partial_docs.end(), docs.begin());

      docs.resize(it - docs.begin());
    }


//    std::set<std::size_t> docs;
//    for (auto &&v : span_cover) {
//      const auto &partial_docs = slp_.GetData(v);
//      docs.insert(partial_docs.begin(), partial_docs.end());
////    std::copy(partial_docs.begin(), partial_docs.end(), out);
//    }

    std::copy(docs.begin(), docs.end(), out);
  }


  template<typename _II, typename _OI>
  void Search(_II begin, _II end, _OI out) {
    std::size_t sp = 1, ep = 0;
    if (0 == backward_search(sa_, 0, sa_.size() - 1, begin, end, sp, ep))
      return;

    SearchInRange(sp, ep + 1, out);
  }


  const _SA &sa() const {
    return sa_;
  }


 private:
  _SA sa_;
  _SLP slp_{0};
};

}

#endif //DRL_PDLODA_H
