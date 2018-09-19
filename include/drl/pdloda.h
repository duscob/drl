//
// Created by Dustin Cobas Batista <dustin.cobas@gmail.com> on 7/20/18.
//

#ifndef DRL_PDLODA_H
#define DRL_PDLODA_H

#include <vector>
#include <fstream>
#include <iterator>

#include <grammar/re_pair.h>
#include <grammar/slp_helper.h>
#include <grammar/sampled_slp.h>
#include <grammar/slp_metadata.h>
#include <grammar/slp.h>
#include <grammar/algorithm.h>

#include "construct_da.h"


namespace drl {

class ConstructSLPAndComputePTS {
 public:
  template<typename _II, typename _Encoder, typename _SLP, typename _PTS>
  void operator()(_II begin, _II end, _Encoder encoder, _SLP &slp, _PTS &pts) const {
    grammar::SLP<> tmp_slp(0);
    auto wrapper = BuildSLPWrapper(tmp_slp);
    encoder.Encode(begin, end, wrapper);

    auto bit_compress = [](sdsl::int_vector<> &_v) { sdsl::util::bit_compress(_v); };
    slp = _SLP(tmp_slp, bit_compress, bit_compress);

    pts.Compute(&slp);
  }
};


class ComputeSpanCoverFromTopFunctor {
 public:
  template<typename _SLP, typename _OutputIterator>
  auto operator()(const _SLP &slp, std::size_t begin, std::size_t end, _OutputIterator out) const {
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

    grammar::AddSet<_PTS> add_set(pts);
    slp.Compute(tmp_slp, 256, add_set, add_set, grammar::MustBeSampled<_PTS>(pts, 16));
  }
};


class ComputeCombinedSLPAndPTS {
 public:
  template<typename _II, typename _Encoder, typename _SLP, typename _PTS>
  void operator()(_II begin, _II end, _Encoder encoder, _SLP &slp, _PTS &pts) const {
    auto wrapper = BuildSLPWrapper(slp);
    encoder.Encode(begin, end, wrapper);

    grammar::AddSet<_PTS> add_set(pts);
    slp.Compute(256, add_set, add_set, grammar::MustBeSampled<_PTS>(pts, 16));
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
    grammar::AddSet<grammar::Chunks<>> add_set(chunks);
    _slp.Compute(tmp_slp, 256, add_set, add_set, grammar::MustBeSampled<grammar::Chunks<>>(chunks, 16));

    auto bit_compress = [](sdsl::int_vector<> &_v) { sdsl::util::bit_compress(_v); };
    _pts = _PTS(chunks, bit_compress, bit_compress);
  }
};


class ComputeSpanCoverFromBottomFunctor {
 public:
  template<typename _SLP, typename _OutputIterator>
  auto operator()(const _SLP &slp, std::size_t begin, std::size_t end, _OutputIterator out) const {
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
    grammar::AddSet<grammar::Chunks<>> add_set(pts);
    _slp.Compute(tmp_slp, 256, add_set, add_set, grammar::MustBeSampled<grammar::Chunks<>>(pts, 16));

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


class SAGetDocs {
 public:
  template<typename _Result, typename _SA, typename _SLP, typename _PTS>
  void operator()(std::size_t _first,
                  std::size_t _last,
                  _Result &_result,
                  const _SA &_sa,
                  const _SLP &_slp,
                  const _PTS &_pts) const {
    _sa.GetDocs(_first, _last, _result);
  }
};


class SLPGetSpan {
 public:
  template<typename _Result, typename _SA, typename _SLP, typename _PTS>
  void operator()(std::size_t _first,
                  std::size_t _last,
                  _Result &_result,
                  const _SA &_sa,
                  const _SLP &_slp,
                  const _PTS &_pts) const {

    if (_first >= _last)
      return;

    auto leaf = _slp.Leaf(_first);
    auto pos = _slp.Position(leaf);

    while (_first < _last) {
      std::vector<uint32_t> vars;
      grammar::ComputeSpanCover(_slp, _first - pos, _last - pos, back_inserter(vars), _slp.Map(leaf));

      for (const auto &v : vars) {
        auto span = _slp.Span(v);
        std::copy(span.begin(), span.end(), back_inserter(_result));
      }

      _first = pos = _slp.Position(++leaf);
    }
  }
};


template<typename _VarType, typename _SLP, typename _OI>
void ComputeSpanFromLeft(_VarType _var, std::size_t &_length, const _SLP &_slp, _OI _out) {
  if (_slp.IsTerminal(_var)) {
    _out = _var;
    ++_out;
    --_length;
    return;
  }

  const auto &children = _slp[_var];
  ComputeSpanFromLeft(children.first, _length, _slp, _out);

  if (_length) {
    ComputeSpanFromLeft(children.second, _length, _slp, _out);
  }
}


template<typename _VarType, typename _SLP, typename _OI>
void ComputeSpanFromFront(_VarType _var, std::size_t _length, const _SLP &_slp, _OI _out) {
  const auto &cover = _slp.Cover(_var);

  auto size = cover.size();

  for (auto it = cover.begin(); _length && size; --size, ++it) {
    ComputeSpanFromLeft(*it, _length, _slp, _out);
  }
}


template<typename _VarType, typename _SLP, typename _OI>
void ComputeSpanFromLeft(_VarType _var, std::size_t &_skip, std::size_t &_length, const _SLP &_slp, _OI _out) {
  if (_slp.IsTerminal(_var)) {
    --_skip;
    return;
  }

  const auto &children = _slp[_var];
  ComputeSpanFromLeft(children.first, _skip, _length, _slp, _out);

  if (_skip) {
    ComputeSpanFromLeft(children.second, _skip, _length, _slp, _out);
  } else if (_length) {
    ComputeSpanFromLeft(children.second, _length, _slp, _out);
  }
}


template<typename _VarType, typename _SLP, typename _OI>
void ComputeSpanFromFront(_VarType _var, std::size_t _skip, std::size_t _length, const _SLP &_slp, _OI _out) {
  const auto &cover = _slp.Cover(_var);

  auto size = cover.size();

  for (auto it = cover.begin(); _length && size; --size, ++it) {
    if (_skip) {
      ComputeSpanFromLeft(*it, _skip, _length, _slp, _out);
    } else {
      ComputeSpanFromLeft(*it, _length, _slp, _out);
    }
  }
}


template<typename _VarType, typename _SLP, typename _OI>
void ComputeSpanFromRight(_VarType _var, std::size_t &_length, const _SLP &_slp, _OI _out) {
  if (_slp.IsTerminal(_var)) {
    _out = _var;
    ++_out;
    --_length;
    return;
  }

  const auto &children = _slp[_var];
  ComputeSpanFromRight(children.second, _length, _slp, _out);

  if (_length) {
    ComputeSpanFromRight(children.first, _length, _slp, _out);
  }
}


template<typename _VarType, typename _SLP, typename _OI>
void ComputeSpanFromBack(_VarType _var, std::size_t _length, const _SLP &_slp, _OI _out) {
  const auto &cover = _slp.Cover(_var);

  auto size = cover.size();

  for (auto it = cover.end() - 1; _length && size; --size, --it) {
    ComputeSpanFromRight(*it, _length, _slp, _out);
  }
}


template<typename _VarType, typename _SLP, typename _OI>
void ComputeSpanFromRight(_VarType _var, std::size_t &_skip, std::size_t &_length, const _SLP &_slp, _OI _out) {
  if (_slp.IsTerminal(_var)) {
    --_skip;
    return;
  }

  const auto &children = _slp[_var];
  ComputeSpanFromRight(children.second, _skip, _length, _slp, _out);

  if (_skip) {
    ComputeSpanFromRight(children.first, _skip, _length, _slp, _out);
  } else if (_length) {
    ComputeSpanFromRight(children.first, _length, _slp, _out);
  }
}


template<typename _VarType, typename _SLP, typename _OI>
void ComputeSpanFromBack(_VarType _var, std::size_t _skip, std::size_t _length, const _SLP &_slp, _OI _out) {
  const auto &cover = _slp.Cover(_var);

  auto size = cover.size();

  for (auto it = cover.end() - 1; _length && size; --size, --it) {
    if (_skip) {
      ComputeSpanFromRight(*it, _skip, _length, _slp, _out);
    } else {
      ComputeSpanFromRight(*it, _length, _slp, _out);
    }
  }
}


class LSLPGetSpan {
 public:
  template<typename _Result, typename _SA, typename _SLP, typename _PTS>
  void operator()(std::size_t _first,
                  std::size_t _last,
                  _Result &_result,
                  const _SA &_sa,
                  const _SLP &_slp,
                  const _PTS &_pts) const {

    if (_first >= _last)
      return;

    auto leaf = _slp.Leaf(_first);
    auto pos = _slp.Position(leaf);

    if (pos == _first) {
      ComputeSpanFromFront(leaf, _last - pos, _slp, back_inserter(_result));
    } else {
      auto next_pos = _slp.Position(leaf + 1);

      if (next_pos <= _last) {
        ComputeSpanFromBack(leaf, next_pos - _first, _slp, back_inserter(_result));

        if (next_pos < _last) {
          ComputeSpanFromFront(leaf + 1, _last - next_pos, _slp, back_inserter(_result));
        }
      } else {
        auto skip_front = _first - pos;
        auto skip_back = next_pos - _last;
        if (skip_front < skip_back) {
          ComputeSpanFromFront(leaf, skip_front, _last - _first, _slp, back_inserter(_result));
        } else {
          ComputeSpanFromBack(leaf, skip_back, _last - _first, _slp, back_inserter(_result));
        }
      }
    }
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
    typename _ComputeRangeTerms = SAGetDocs/*,
    typename _BitVectorDocBorder = sdsl::bit_vector,
    typename _BitVectorDocBorderRank = typename _BitVectorDocBorder::rank_1_type*/>
class PDLODA {
 public:
  template</*typename _E, */typename _Constructor, typename _LoadSLPAndPTS = DefaultLoadSLPAndPTS>
  PDLODA(/*const std::string &filename,
         const _E &doc_delim,*/
      const _SA &_sa,
      sdsl::cache_config &cconfig,
      _Constructor _construct,
      _LoadSLPAndPTS _load = DefaultLoadSLPAndPTS(),
      _ComputeSpanCover _compute_cover = _ComputeSpanCover()): sa_(_sa), cover_(_compute_cover) {
//    uint8_t num_bytes = sizeof(_E);
//    std::string id = sdsl::util::basename(filename) + "_";
//    sdsl::cache_config cconfig{false, "./", id};
//    construct(sa_, filename, cconfig, num_bytes);
//
//    if (sdsl::cache_file_exists<_BitVectorDocBorder>("doc_border_", cconfig)) {
//      sdsl::load_from_file(doc_border_, sdsl::cache_file_name<_BitVectorDocBorder>("doc_border_", cconfig));
//    } else {
//      std::ifstream in(filename, std::ios::binary);
//      drl::ConstructDocBorder(std::istream_iterator<_E>(in >> std::noskipws),
//                              std::istream_iterator<_E>{},
//                              doc_border_,
//                              doc_delim,
//                              sa_.size());
//
//      sdsl::store_to_file(doc_border_, sdsl::cache_file_name<_BitVectorDocBorder>("doc_border_", cconfig));
//    }
//    doc_border_rank_ = _BitVectorDocBorderRank(&doc_border_);

    if (sdsl::cache_file_exists<_SLP>("slp", cconfig) && sdsl::cache_file_exists<_PTS>("pts", cconfig)) {
      std::cout << "Load SLP & PTS" << std::endl;
      _load(slp_, pts_, cconfig);
    } else {
      std::cout << "Compute SLP & PTS" << std::endl;
//      sdsl::int_vector<> doc_array(sa_.size() + 1); //todo change by std::vector<std::size_t>
//      drl::ConstructDocArray(sa_, doc_border_rank_, doc_array);
      std::vector<uint32_t> doc_array;
      doc_array.reserve(sa_.size() + 1);
//      sa_.GetDocs(0, sa_.size(), doc_array);
      sa_.GetDA(doc_array);

      std::cout << "GetDocs" << std::endl;

      grammar::RePairEncoder<true> encoder;
      _construct(doc_array.begin(), doc_array.end(), encoder, slp_, pts_);

      std::cout << "RePair" << std::endl;

      sdsl::store_to_file(slp_, sdsl::cache_file_name<_SLP>("slp", cconfig));
      sdsl::store_to_file(pts_, sdsl::cache_file_name<_PTS>("pts", cconfig));
    }

//    std::cout << "slp_.Sigma() = " << slp_.Sigma() << std::endl;
//    std::cout << "slp_.Start() = " << slp_.Start() << std::endl;
//    std::cout << "slp_.SpanLength(slp_.Start()) = " << slp_.SpanLength(slp_.Start()) << std::endl;
  }

//  template<typename _OI>
  auto SearchInRange(std::size_t begin, std::size_t end/*, _OI out*/) {
    std::vector<std::size_t> span_cover;
    auto range = cover_(slp_, begin, end, back_inserter(span_cover));

    std::vector<uint32_t> docs;
    docs.reserve(range.first - begin + end - range.second);
    if (span_cover.empty()) {
//      for (auto i = begin; i < end; ++i) {
//        docs.emplace_back(doc_border_rank_(sa_[i]) + 1);
//      }
//      sa_.GetDocs(begin, end, docs);
      get_terms_(begin, end, docs, sa_, slp_, pts_);
    } else {
//      for (auto i = begin; i < range.first; ++i) {
//        docs.emplace_back(doc_border_rank_(sa_[i]) + 1);
//      }
//      sa_.GetDocs(begin, range.first, docs);
      get_terms_(begin, range.first, docs, sa_, slp_, pts_);
//      for (auto i = range.second; i < end; ++i) {
//        docs.emplace_back(doc_border_rank_(sa_[i]) + 1);
//      }
//      sa_.GetDocs(range.second, end, docs);
      get_terms_(range.second, end, docs, sa_, slp_, pts_);
    }
    sort(docs.begin(), docs.end());
    docs.erase(unique(docs.begin(), docs.end()), docs.end());

    if (span_cover.empty()) {
      return docs;
    }

//    grammar::MergeSetsOneByOne(span_cover.begin(), span_cover.end(), pts_, docs);
    grammar::MergeSetsBinaryTree(span_cover.begin(), span_cover.end(), pts_, docs);


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
  const _SA &sa_;

//  _BitVectorDocBorder doc_border_;
//  _BitVectorDocBorderRank doc_border_rank_;

  _SLP slp_;
  _PTS pts_;

  _ComputeSpanCover cover_;

  _ComputeRangeTerms get_terms_;
};


template<typename _SA, typename _SLP, typename _PTS, typename _ComputeSpanCover, typename _ComputeRangeTerms = SAGetDocs>
class PDLGT {
 public:
  typedef std::size_t size_type;

  PDLGT(const _SA &_sa, const _SLP &_slp, const _PTS &_pts, _ComputeSpanCover _cover, _ComputeRangeTerms _get_terms)
      : sa_(_sa), slp_(_slp), pts_(_pts), cover_(_cover), get_terms_(_get_terms) {}

  auto SearchInRange(std::size_t _first, std::size_t _last) {
    std::vector<std::size_t> span_cover;
    auto range = cover_(slp_, _first, _last, back_inserter(span_cover));

    std::vector<uint32_t> docs;
    docs.reserve(range.first - _first + _last - range.second);
    if (span_cover.empty()) {
      get_terms_(_first, _last, docs, sa_, slp_, pts_);
    } else {
      get_terms_(_first, range.first, docs, sa_, slp_, pts_);
      get_terms_(range.second, _last, docs, sa_, slp_, pts_);
    }
    sort(docs.begin(), docs.end());
    docs.erase(unique(docs.begin(), docs.end()), docs.end());

    if (span_cover.empty()) {
      return docs;
    }

    if (span_cover.size() < 20)
      grammar::MergeSetsOneByOne(span_cover.begin(), span_cover.end(), pts_, docs);
    else {
//       grammar::MergeSetsBinaryTree(span_cover.begin(), span_cover.end(), pts_, docs);

      auto _set_union = [](auto _first1, auto _last1, auto _first2, auto _last2, auto _result) -> auto {
        return std::set_union(_first1, _last1, _first2, _last2, _result);
      };
      typedef decltype(docs) _Result;
      auto _first = span_cover.begin();
      auto _last = span_cover.end();
      auto &_sets = pts_;
      auto &_result = docs;

      _Result tmp_merge;

      auto merge_tmp = [&tmp_merge, &_set_union](const auto &set1, const auto &set2) {
        tmp_merge.resize(set1.size() + set2.size());
        auto last_it = _set_union(set1.begin(), set1.end(), set2.begin(), set2.end(), tmp_merge.begin());
        tmp_merge.resize(last_it - tmp_merge.begin());
      };

      auto length = std::distance(_first, _last);
      if (length == 1) {
        merge_tmp(_result, _sets[*_first]);

        _result.swap(tmp_merge);

        return _result;
      }

      std::vector<std::pair<uint8_t, _Result>> part_results = {{1, {}}};
      part_results.front().second.swap(_result);

      while (part_results.size() != 1 || _first != _last) {
        std::size_t size;
        while ((size = part_results.size()) > 1
            && (part_results[size - 1].first == part_results[size - 2].first
                || _first == _last)) {
          merge_tmp(part_results[size - 1].second, part_results[size - 2].second);

          if (slp_.Sigma() < tmp_merge.size()) {
            _result.swap(tmp_merge);

            return _result;
          }

          part_results[size - 2].second.swap(tmp_merge);
          ++part_results[size - 2].first;
          part_results.pop_back();
        }

        if (_first != _last) {
          auto next = _first + 1;
          if (next == _last) {
            merge_tmp(part_results.back().second, _sets[*_first]);

            if (slp_.Sigma() < tmp_merge.size()) {
              _result.swap(tmp_merge);

              return _result;
            }

            part_results.back().second.swap(tmp_merge);
            ++_first;
          } else {
            merge_tmp(_sets[*_first], _sets[*next]);

            if (slp_.Sigma() < tmp_merge.size()) {
              _result.swap(tmp_merge);

              return _result;
            }

            part_results.emplace_back(1, std::move(tmp_merge));
            _first += 2;
          }
        }
      }

      _result.swap(part_results.front().second);
    }

    sort(docs.begin(), docs.end());
    docs.erase(unique(docs.begin(), docs.end()), docs.end());

    return docs;
  }

  std::size_t serialize(std::ostream &out, sdsl::structure_tree_node *v = nullptr, std::string name = "") const {
    std::size_t written_bytes = 0;
    written_bytes += sdsl::serialize(slp_, out);
    written_bytes += sdsl::serialize(pts_, out);

    return written_bytes;
  }

  void load(std::istream &in) {
    sdsl::load(slp_, in);
    sdsl::load(pts_, in);
  }

 protected:
  const _SA &sa_;

  const _SLP &slp_;
  const _PTS &pts_;

  _ComputeSpanCover cover_;

  _ComputeRangeTerms get_terms_;
};


template<typename _SA, typename _SLP, typename _PTS, typename _ComputeSpanCover, typename _ComputeRangeTerms = SAGetDocs>
auto BuildPDLGT(const _SA &_sa,
                const _SLP &_slp,
                const _PTS &_pts,
                _ComputeSpanCover _cover,
                _ComputeRangeTerms _get_terms) {
  return PDLGT<_SA, _SLP, _PTS, _ComputeSpanCover, _ComputeRangeTerms>(_sa, _slp, _pts, _cover, _get_terms);
}

}

#endif //DRL_PDLODA_H
