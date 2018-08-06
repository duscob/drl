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
  _Result tmp_set;

  for (auto it = _first; it != _last; ++it) {
    const auto &set = _sets[*it];

    tmp_set.resize(_result.size() + set.size());
    tmp_set.swap(_result);

    auto last_it = _set_union(tmp_set.begin(), tmp_set.end(), set.begin(), set.end(), _result.begin());
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

    return;
  }

  std::vector<std::pair<uint8_t, _Result>> part_results = {{1, {}}};
  part_results.front().second.swap(_result);

  while (part_results.size() != 1 || _first != _last) {
    std::size_t size;
    while ((size = part_results.size()) > 1
        && (part_results[size - 1].first == part_results[size - 2].first
            || _first == _last)) {
      merge_tmp(part_results[size - 1].second, part_results[size - 2].second);

      part_results[size - 2].second.swap(tmp_merge);
      ++part_results[size - 2].first;
      part_results.pop_back();
    }

    if (_first != _last) {
      auto next = _first + 1;
      if (next == _last) {
        merge_tmp(part_results.back().second, _sets[*_first]);

        part_results.back().second.swap(tmp_merge);
        ++_first;
      } else {
        merge_tmp(_sets[*_first], _sets[*next]);

        part_results.emplace_back(1, std::move(tmp_merge));
        _first += 2;
      }
    }
  }

  _result.swap(part_results.front().second);
}

};

#endif //DRL_MERGE_SETS_H
