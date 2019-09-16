//
// Created by Dustin Cobas Batista <dustin.cobas@gmail.com> on 2019-08-20.
//

#ifndef DRL_RLE_H_
#define DRL_RLE_H_

#include <cstddef>
#include <vector>
#include <iterator>

namespace drl {

/***
 * Build the Run-Length Encoding bitvector for a given sequence
 * @tparam _II Input Iterator
 * @tparam _BVRuns BitVector to store the runs of equal values
 *
 * @param _first
 * @param _last
 * @param _bv_runs
 */
template<typename _II, typename _BVRuns>
void BuildRLEncoding(_II _first, _II _last, _BVRuns &_bv_runs) {
  if (_first == _last) return;

  _bv_runs = _BVRuns(distance(_first, _last));
  std::size_t i = 0;
  _bv_runs[i] = 0;
  for (auto it = _first++; _first != _last; ++it, ++_first) {
    _bv_runs[++i] = (*it == *_first);
  }
}


/**
 * Run-Length Encoding
 * @tparam _BVRuns BitVector to store the runs of equal values
 * @tparam _BVRunsRank Rank-support over the bitvector with the runs
 */
template<typename _BVRuns, typename _BVRunsRank = typename _BVRuns::rank_1_type>
class RLEncoding {
 public:
  RLEncoding() = default;

  template<typename __BVRuns>
  RLEncoding(const __BVRuns &_bv_runs): bv_runs_(_bv_runs), bv_runs_rank_(&bv_runs_) {}

  template<typename __BVRuns, typename __BVRunsRank>
  RLEncoding(RLEncoding<__BVRuns, __BVRunsRank> _rle): bv_runs_(_rle.GetRuns()), bv_runs_rank_(&bv_runs_) {}

  /**
   * Compute the new position on the run-length encoding sequence
   *
   * @param _pos Original Position
   * @return Run-Length Encoding Position
   */
  std::size_t operator()(std::size_t _pos) const {
    return _pos - bv_runs_rank_(_pos + 1);
  }

  const _BVRuns & GetRuns() const {
    return bv_runs_;
  }

  typedef std::size_t size_type;

  template<typename ..._Args>
  size_type serialize(std::ostream &out, _Args ..._args/*sdsl::structure_tree_node *v = nullptr, const std::string &name = ""*/) const {
    std::size_t written_bytes = 0;
    written_bytes += bv_runs_.serialize(out);
    written_bytes += bv_runs_rank_.serialize(out);

    return written_bytes;
  }

  void load(std::istream &in) {
    bv_runs_.load(in);
    bv_runs_rank_.load(in, &bv_runs_);
  }

 protected:
  _BVRuns bv_runs_;
  _BVRunsRank bv_runs_rank_;
};


template<typename _Iter>
class RLEIterator : public std::iterator<std::input_iterator_tag,
                                         typename _Iter::value_type,
                                         typename _Iter::difference_type,
                                         typename _Iter::pointer,
                                         typename _Iter::reference> {
 public:
  RLEIterator(const _Iter &_iter, _Iter _eiter) : iter_(_iter), eiter_(_eiter) {}

  RLEIterator &operator++() {
    for (auto it = iter_++; iter_ != eiter_ && *it == *iter_; ++it, ++iter_);

    return *this;
  }

  RLEIterator operator++(int) {
    _Iter first = iter_;
    for (auto it = iter_++; iter_ != eiter_ && *it == *iter_; ++it, ++iter_);

    return RLEIterator<_Iter>(first, eiter_);
  }

  bool operator==(RLEIterator _other) const { return iter_ == _other.iter_; }

  bool operator!=(RLEIterator _other) const { return !(*this == _other); }

  typename _Iter::reference operator*() const { return *iter_; }

  typename _Iter::pointer operator->() const { return iter_.operator->(); }

 protected:
  _Iter iter_;
  _Iter eiter_;
};

}

#endif //DRL_RLE_H_
