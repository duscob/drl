//
// Created by Dustin Cobas Batista <dustin.cobas@gmail.com> on 7/20/18.
//

#ifndef DRL_PDLODA_H
#define DRL_PDLODA_H

#include <vector>

#include <grammar/re_pair.h>
#include <grammar/slp_helper.h>
#include <grammar/sampled_slp.h>
#include <grammar/slp_metadata.h>
#include <grammar/slp.h>

#include "construct_da.h"
#include "merge_sets.h"


namespace drl {

class ConstructSLPAndComputePTS {
 public:
  template<typename _II, typename _Encoder, typename _SLP, typename _PTS>
  void operator()(_II begin, _II end, _Encoder encoder, _SLP &slp, _PTS &pts) const {
    auto wrapper = BuildSLPWrapper(slp);
    encoder.Encode(begin, end, wrapper);

    pts.Compute(&slp);
  }
};


class ComputeSpanCoverFromTopWrapper {
 public:
  template<typename _SLP, typename _OutputIterator>
  std::pair<std::size_t, std::size_t> operator()(const _SLP &slp,
                                                 std::size_t begin,
                                                 std::size_t end,
                                                 _OutputIterator out) const {
    return ComputeSpanCover(slp, begin, end, out);
  }
};


class ComputeSLPAndPTS {
 public:
  template<typename _II, typename _Encoder, typename _SLP, typename _PTS>
  void operator()(_II begin, _II end, _Encoder encoder, _SLP &slp, _PTS &pts) const {
    grammar::SLP<> tmp_slp(0);
    auto wrapper = BuildSLPWrapper(tmp_slp);
    encoder.Encode(begin, end, wrapper);

    slp.Compute(tmp_slp, 256, grammar::AddSet<_PTS>(pts), grammar::MustBeSampled<_PTS>(pts, 16));
  }
};


class ComputeSLPAndActionPTS {
 public:
  template<typename _II, typename _Encoder, typename _SLP, typename _PTS>
  void operator()(_II _first, _II _last, _Encoder _encoder, _SLP &_slp, _PTS &_pts) const {
    grammar::SLP<> tmp_slp(0);
    auto wrapper = BuildSLPWrapper(tmp_slp);
    _encoder.Encode(_first, _last, wrapper);

    grammar::Chunks<> chunks;
    _slp.Compute(tmp_slp,
                 256,
                 grammar::AddSet<grammar::Chunks<>>(chunks),
                 grammar::MustBeSampled<grammar::Chunks<>>(chunks, 16));

    auto bit_compress = [](sdsl::int_vector<> &_v) { sdsl::util::bit_compress(_v); };
    _pts = _PTS(chunks, bit_compress, bit_compress);
  }
};


class ComputeSpanCoverFromBottomWrapper {
 public:
  template<typename _SLP, typename _OutputIterator>
  std::pair<std::size_t, std::size_t> operator()(const _SLP &slp,
                                                 std::size_t begin,
                                                 std::size_t end,
                                                 _OutputIterator out) const {
    return ComputeSpanCoverFromBottom(slp, begin, end, out);
  }
};


class ComputeSLPAndCompactPTS {
 public:
  template<typename _II, typename _Encoder, typename _SLP, typename _PTS>
  void operator()(_II begin, _II end, _Encoder encoder, _SLP &_slp, _PTS &_pts) const {
    grammar::SLP<> tmp_slp(0);
    auto wrapper = BuildSLPWrapper(tmp_slp);
    encoder.Encode(begin, end, wrapper);

    grammar::Chunks<> pts;
    _slp.Compute(tmp_slp,
                 256,
                 grammar::AddSet<grammar::Chunks<>>(pts),
                 grammar::MustBeSampled<grammar::Chunks<>>(pts, 16));

    grammar::RePairEncoder<false> encoder1;
    _pts.Compute(pts.GetObjects().begin(), pts.GetObjects().end(), pts, encoder1);
  }
};


class DefaultLoadSLPAndPTS {
 public:
  template<typename _SLP, typename _PTS>
  void operator()(_SLP &_slp, _PTS &_pts, const sdsl::cache_config &_cconfig) const {
    sdsl::load_from_file(_slp, sdsl::cache_file_name<_SLP>("slp", _cconfig));
    sdsl::load_from_file(_pts, sdsl::cache_file_name<_PTS>("pts", _cconfig));
  }
};


class LoadSLPAndPTS {
 public:
  template<typename _SLP, typename _PTS>
  void operator()(_SLP &_slp, _PTS &_pts, const sdsl::cache_config &_cconfig) const {
    sdsl::load_from_file(_slp, sdsl::cache_file_name<_SLP>("slp", _cconfig));
    sdsl::load_from_file(_pts, sdsl::cache_file_name<_PTS>("pts", _cconfig));

    _pts.SetSLP(&_slp);
  }
};


//! Precomputed Document List On Document Array
/**
 *
 * @tparam _SA Suffix Array
 * @tparam _SLP Straight-Line Program
 */
template<typename _SA,
    typename _SLP,
    typename _PTS,
    typename _ComputeSpanCover,
    typename _BitVectorDocBorder = sdsl::bit_vector,
    typename _BitVectorDocBorderRank = typename _BitVectorDocBorder::rank_1_type>
class PDLODA {
 public:
  template<typename _E = int8_t, typename _Constructor, typename _LoadSLPAndPTS = DefaultLoadSLPAndPTS>
  PDLODA(const std::string &filename,
         const _E &doc_delim,
         _Constructor _construct,
         _LoadSLPAndPTS _load = DefaultLoadSLPAndPTS(),
         _ComputeSpanCover _compute_cover = _ComputeSpanCover()) {
    uint8_t num_bytes = sizeof(_E);
    std::string id = sdsl::util::basename(filename) + "_";
    sdsl::cache_config cconfig{false, "./", id};
    construct(sa_, filename, cconfig, num_bytes);

    if (sdsl::cache_file_exists<_BitVectorDocBorder>("doc_border_", cconfig)) {
      sdsl::load_from_file(doc_border_, sdsl::cache_file_name<_BitVectorDocBorder>("doc_border_", cconfig));
    } else {
      std::ifstream in(filename, std::ios::binary);
      drl::ConstructDocBorder(std::istream_iterator<_E>(in),
                              std::istream_iterator<_E>{},
                              doc_border_,
                              doc_delim,
                              sa_.size());

      sdsl::store_to_file(doc_border_, sdsl::cache_file_name<_BitVectorDocBorder>("doc_border_", cconfig));
    }
    doc_border_rank_ = _BitVectorDocBorderRank(&doc_border_);

    if (sdsl::cache_file_exists<_SLP>("slp", cconfig) && sdsl::cache_file_exists<_PTS>("pts", cconfig)) {
      _load(slp_, pts_, cconfig);
    } else {
      sdsl::int_vector<> doc_array(sa_.size() + 1);
      drl::ConstructDocArray(sa_, doc_border_rank_, doc_array);

      grammar::RePairEncoder<true> encoder;
      _construct(doc_array.begin(), doc_array.end(), encoder, slp_, pts_);

      sdsl::store_to_file(slp_, sdsl::cache_file_name<_SLP>("slp", cconfig));
      sdsl::store_to_file(pts_, sdsl::cache_file_name<_PTS>("pts", cconfig));
    }
  }

//  template<typename _OI>
  auto SearchInRange(std::size_t begin, std::size_t end/*, _OI out*/) {
    std::vector<std::size_t> span_cover;
    auto range = cover_(slp_, begin, end, back_inserter(span_cover));

    std::vector<uint32_t> docs;
    docs.reserve(range.first - begin + end - range.second);
    if (span_cover.empty()) {
      for (auto i = begin; i < end; ++i) {
        docs.emplace_back(doc_border_rank_(sa_[i]) + 1);
      }
    } else {
      for (auto i = begin; i < range.first; ++i) {
        docs.emplace_back(doc_border_rank_(sa_[i]) + 1);
      }
      for (auto i = range.second; i < end; ++i) {
        docs.emplace_back(doc_border_rank_(sa_[i]) + 1);
      }
    }
    sort(docs.begin(), docs.end());
    docs.erase(unique(docs.begin(), docs.end()), docs.end());

    if (span_cover.empty()) {
      return docs;
    }

    drl::MergeSetsOneByOne(span_cover.begin(), span_cover.end(), pts_, docs);

//    std::set<std::size_t> docs;
//    for (auto &&v : span_cover) {
//      const auto &partial_docs = slp_.GetData(v);
//      docs.insert(partial_docs.begin(), partial_docs.end());
////    std::copy(partial_docs.begin(), partial_docs.end(), out);
//    }

//    std::copy(docs.begin(), docs.end(), out);
    return docs;
  }

  void SpanCover(std::size_t begin, std::size_t end) {
    std::vector<std::size_t> span_cover;
    cover_(slp_, begin, end, back_inserter(span_cover));

    if (span_cover.empty())
      return;

    std::vector<bool> set(150000, 0);
    std::vector<uint32_t> docs;
//    docs.reserve()
    for (const auto &item : span_cover) {
      const auto &partial_docs = pts_[item];
      for (const auto &doc : partial_docs) {
//        std::cout << doc << " " << std::endl;
//        docs.emplace_back(doc);
        set[doc] = 1;
      }
    }

    for (int i = 0; i < set.size(); ++i) {
      if (set[i])
        docs.emplace_back(i);
    }

//    sort(docs.begin(), docs.end());
//    docs.erase(unique(docs.begin(), docs.end()), docs.end());
//    std::cout << std::endl;

//    return docs;
  }

  template<typename _II, typename _OI>
  void Search(_II begin, _II end, _OI out) {
    std::size_t sp = 1, ep = 0;
    if (0 == backward_search(sa_, 0, sa_.size() - 1, begin, end, sp, ep))
      return;

    auto res = SearchInRange(sp, ep + 1/*, out*/);
    copy(res.begin(), res.end(), out);
  }

  const _SA &sa() const {
    return sa_;
  }

 private:
  _SA sa_;

  _BitVectorDocBorder doc_border_;
  _BitVectorDocBorderRank doc_border_rank_;

  _SLP slp_;
  _PTS pts_;

  _ComputeSpanCover cover_;
};

}

#endif //DRL_PDLODA_H
