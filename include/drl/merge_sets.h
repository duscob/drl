//
// Created by Dustin Cobas Batista <dustin.cobas@gmail.com> on 8/5/18.
//

#ifndef DRL_MERGE_SETS_H
#define DRL_MERGE_SETS_H

#include <type_traits>
#include <algorithm>


namespace drl {

template<typename _II, typename _Sets, typename _Result>
void MergeSetsOneByOne(_II _first, _II _last, const _Sets &_sets, _Result &_set) {
  auto default_set_union = [](auto _first1, auto _last1, auto _first2, auto _last2, auto _result) -> auto {
    return std::set_union(_first1, _last1, _first2, _last2, _result);
  };

  MergeSetsOneByOne(_first, _last, _sets, _set, default_set_union);
}


template<typename _II, typename _Sets, typename _Result, typename _SetUnion>
void MergeSetsOneByOne(_II _first, _II _last, const _Sets &_sets, _Result &_set, _SetUnion _set_union) {
  typename std::remove_reference<decltype(_set)>::type tmp_set;

  for (auto it = _first; it != _last; ++it) {
    const auto &partial_docs = _sets[*it];

    tmp_set.resize(_set.size() + partial_docs.size());
    tmp_set.swap(_set);

    auto last_it = _set_union(tmp_set.begin(), tmp_set.end(), partial_docs.begin(), partial_docs.end(), _set.begin());

    _set.resize(last_it - _set.begin());
  }
}

}

#endif //DRL_MERGE_SETS_H
