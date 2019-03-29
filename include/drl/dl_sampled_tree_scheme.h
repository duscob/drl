//
// Created by Dustin Cobas Batista <dustin.cobas@gmail.com> on 2/15/19.
//

#ifndef DRL_DL_SAMPLED_TREE_H
#define DRL_DL_SAMPLED_TREE_H

#include <cstddef>
#include <cstdint>
#include <vector>
#include <algorithm>

namespace drl {

template<typename _ComputeCover, typename _GetDocs, typename _GetDocSet, typename _MergeSets>
class DLSampledTreeScheme {
 public:
  auto list(std::size_t _sp, std::size_t _ep) {
    auto cover = compute_cover_(_sp, _ep);

    const auto &range = cover.first;
    const auto &nodes = cover.second;

    std::vector<uint32_t> docs;
    docs.reserve(range.first - _sp + _ep - range.second);

    auto add_doc = [&docs](auto d) { docs.push_back(d); };

    if (nodes.empty()) {
      get_docs_(_sp, _ep, add_doc);
    } else {
      get_docs_(_sp, range.first, add_doc);
      get_docs_(range.second, _ep, add_doc);
    }

    sort(docs.begin(), docs.end());
    docs.erase(unique(docs.begin(), docs.end()), docs.end());

    if (nodes.empty()) {
      return docs;
    }

    merge_sets_(nodes.begin(), nodes.end(), get_doc_set_, docs);

    return docs;
  }

 protected:
  _ComputeCover compute_cover_;
  _GetDocs get_docs_;
  _GetDocSet get_doc_set_;
  _MergeSets merge_sets_;
};


template<typename _Tree, typename _OI>
auto ComputeCoverFromBottom(const _Tree &_tree, std::size_t _sp, std::size_t _ep, _OI out)
-> std::pair<decltype(_tree.Position(1)), decltype(_tree.Position(1))> {
  auto l = _tree.Leaf(_sp);
  if (_tree.Position(l) < _sp) ++l;

  auto r = _tree.Leaf(_ep) - 1;

  for (auto i = l, next = i + 1; i <= r; i = next, ++next) {
    while (_tree.IsFirstChild(i)) {
      auto p = _tree.Parent(i);
      if (p.second > r + 1)
        break;
      i = p.first;
      next = p.second;
    }

    out = i;
    ++out;
  }

  return std::make_pair(_tree.Position(l), _tree.Position(r + 1));
}


class PDLSampledTree {
 public:

 protected:

};

}

#endif //DRL_DL_SAMPLED_TREE_H
