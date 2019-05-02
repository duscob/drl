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

    auto add_doc = [&docs](auto &&_d) { docs.emplace_back(_d); };

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

}

#endif //DRL_DL_SAMPLED_TREE_H
