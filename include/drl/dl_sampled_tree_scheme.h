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
  DLSampledTreeScheme(const _ComputeCover &_compute_cover,
                      const _GetDocs &_get_docs,
                      const _GetDocSet &_get_doc_set,
                      const _MergeSets &_merge_set) : compute_cover_{_compute_cover},
                                                      get_docs_{_get_docs},
                                                      get_doc_set_{_get_doc_set},
                                                      merge_sets_{_merge_set} {
  }

  auto list(std::size_t _sp, std::size_t _ep) const {
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
  const _ComputeCover &compute_cover_;
  const _GetDocs &get_docs_;
  const _GetDocSet &get_doc_set_;
  const _MergeSets &merge_sets_;
};


template<typename _ComputeCover, typename _GetDocs, typename _GetDocSet, typename _MergeSets>
auto BuildDLSampledTreeScheme(const _ComputeCover &_compute_cover,
                              const _GetDocs &_get_docs,
                              const _GetDocSet &_get_doc_set,
                              const _MergeSets &_merge_set) {
  return DLSampledTreeScheme<_ComputeCover, _GetDocs, _GetDocSet, _MergeSets>(_compute_cover,
                                                                              _get_docs,
                                                                              _get_doc_set,
                                                                              _merge_set);
}


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


template<typename _Tree>
class ComputeCoverBottomFunctor {
 public:
  ComputeCoverBottomFunctor(const _Tree &_tree) : tree_(_tree) {}

  auto Compute(std::size_t _sp, std::size_t _ep) const {
    std::vector<std::size_t> nodes;

    auto range = ComputeCoverFromBottom(tree_, _sp, _ep, back_inserter(nodes));

    return std::make_pair(range, nodes);
  }

  auto operator()(std::size_t _sp, std::size_t _ep) const {
    return Compute(_sp, _ep);
  }

 protected:
  const _Tree &tree_;
};


template<typename _Tree>
auto BuildComputeCoverBottomFunctor(const _Tree &_tree) {
  return ComputeCoverBottomFunctor<_Tree>(_tree);
}

}

#endif //DRL_DL_SAMPLED_TREE_H
