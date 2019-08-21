//
// Created by Dustin Cobas Batista <dustin.cobas@gmail.com> on 2019-08-20.
//

#ifndef DRL_RLE_H_
#define DRL_RLE_H_

#include <cstddef>
#include <vector>

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
  template<typename __BVRuns>
  RLEncoding(const __BVRuns &_bv_runs): bv_runs_(_bv_runs), bv_runs_rank_(&bv_runs_) {}

  /**
   * Compute the new position on the run-length encoding sequence
   *
   * @param _pos Original Position
   * @return Run-Length Encoding Position
   */
  std::size_t operator()(std::size_t _pos) {
    return _pos - bv_runs_rank_(_pos + 1);
  }

 protected:
  _BVRuns bv_runs_;
  _BVRunsRank bv_runs_rank_;
};

}

#endif //DRL_RLE_H_
