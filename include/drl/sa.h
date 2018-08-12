//
// Created by Dustin Cobas Batista <dustin.cobas@gmail.com> on 8/11/18.
//

#ifndef DRL_SA_H
#define DRL_SA_H

#include <string>

#include <sdsl/config.hpp>
#include <sdsl/int_vector.hpp>

#include <rlcsa/rlcsa.h>


namespace drl {

template<typename _CSA,
    typename _BitVectorDocBorder = sdsl::bit_vector,
    typename _BitVectorDocBorderRank = typename _BitVectorDocBorder::rank_1_type>
class CSAWrapper {
 public:
  template<typename _E>
  CSAWrapper(const std::string &_filename, const _E &_doc_delim, sdsl::cache_config &_cconfig) {
    uint8_t num_bytes = sizeof(_E);
    construct(csa_, _filename, _cconfig, num_bytes);

    if (sdsl::cache_file_exists<_BitVectorDocBorder>("doc_border_", _cconfig)) {
      sdsl::load_from_file(doc_border_, sdsl::cache_file_name<_BitVectorDocBorder>("doc_border_", _cconfig));
    } else {
      std::size_t sp = 1, ep = 0;
      if (0 == backward_search(csa_, 0, csa_.size() - 1, _doc_delim, sp, ep)) {
        return;
      }

      doc_border_ = _BitVectorDocBorder(csa_.size(), 0);

      std::vector<std::size_t> pos;
      pos.reserve(ep - sp + 1);
      for (auto i = sp; i <= ep; ++i) {
        pos.emplace_back(csa_[i]);
      }
      sort(pos.begin(), pos.end());

//      std::cout << "Pos by CSA: " << std::endl;
//      std::size_t i = 0;
      for (const auto &item : pos) {
//        std::cout << ++i << "-" << item << ", ";
        doc_border_[item] = 1;
      }
//      std::cout << std::endl;

      sdsl::store_to_file(doc_border_, sdsl::cache_file_name<_BitVectorDocBorder>("doc_border_", _cconfig));
    }
    doc_border_rank_ = _BitVectorDocBorderRank(&doc_border_);
  }

  auto size() const {
    return csa_.size();
  }

  const auto &GetSA() const {
    return csa_;
  }

  template<typename _Result>
  void GetDocs(std::size_t _first, std::size_t _last, _Result &_result) const {
    for (auto i = _first; i < _last; ++i) {
      _result.emplace_back(doc_border_rank_(csa_[i]) + 1);
    }
  }

 private:
  _CSA csa_;

  _BitVectorDocBorder doc_border_;
  _BitVectorDocBorderRank doc_border_rank_;
};


class RLCSAWrapper {
 public:
  RLCSAWrapper(const CSA::RLCSA &_rlcsa): rlcsa_(_rlcsa) {}

  auto size() const {
    return rlcsa_.getSize();
  }

  template<typename _Result>
  void GetDocs(std::size_t _first, std::size_t _last, _Result &_result) const {
    bruteForceDocList(rlcsa_, CSA::pair_type(_first, _last - 1), &_result);
  }

 private:
  const CSA::RLCSA &rlcsa_;
};

}

#endif //DRL_SA_H
