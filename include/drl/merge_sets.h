//
// Created by Dustin Cobas Batista <dustin.cobas@gmail.com> on 8/5/18.
//

#ifndef DRL_MERGE_SETS_H
#define DRL_MERGE_SETS_H

#include <type_traits>
#include <algorithm>


namespace drl {

template<typename _II, typename _Sets, typename _Result>
void MergeSetsOneByOne(_II _first, _II _last, const _Sets &_sets, _Result &_result) {
  auto default_set_union = [](auto _first1, auto _last1, auto _first2, auto _last2, auto _result) -> auto {
    return std::set_union(_first1, _last1, _first2, _last2, _result);
  };

  MergeSetsOneByOne(_first, _last, _sets, _result, default_set_union);
}


template<typename _II, typename _Sets, typename _Result, typename _SetUnion>
void MergeSetsOneByOne(_II _first, _II _last, const _Sets &_sets, _Result &_result, _SetUnion _set_union) {
  typename std::remove_reference<decltype(_result)>::type tmp_set;

  for (auto it = _first; it != _last; ++it) {
    const auto &partial_docs = _sets[*it];

    tmp_set.resize(_result.size() + partial_docs.size());
    tmp_set.swap(_result);

    auto last_it = _set_union(tmp_set.begin(), tmp_set.end(), partial_docs.begin(), partial_docs.end(), _result.begin());

    _result.resize(last_it - _result.begin());
  }
}

// todo
class MergeSetsOneByOneFunctor {
 public:
};


template<typename _II, typename _Sets, typename _Result>
void MergeSetsBinaryTree(_II _first, _II _last, const _Sets &_sets, _Result &_result) {
  auto default_set_union = [](auto _first1, auto _last1, auto _first2, auto _last2, auto _result) -> auto {
    return std::set_union(_first1, _last1, _first2, _last2, _result);
  };

  MergeSetsBinaryTree(_first, _last, _sets, _result, default_set_union);
}


template<typename _II, typename _Sets, typename _Result, typename _SetUnion>
void MergeSetsBinaryTree(_II _first, _II _last, const _Sets &_sets, _Result &_result, _SetUnion _set_union) {
  std::vector<std::vector<std::size_t>> part_result; // todo change std::size_t by real value_type

  auto length = std::distance(_first, _last);
  if (length > 1) {
    for (auto next = _first + 1; next != _last; _first = next, ++next) {

    }
  }
}

};

#endif //DRL_MERGE_SETS_H
