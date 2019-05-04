//
// Created by Dustin Cobas Batista<dustin.cobas@gmail.com> on 4/24/19.
//

#ifndef DRL_INCLUDE_DRL_HELPER_H_
#define DRL_INCLUDE_DRL_HELPER_H_

#include <vector>
#include <cstddef>
#include <algorithm>

#include <grammar/slp_helper.h>


namespace drl {

template<typename _Tree, typename _Report>
auto ComputeCoverFromBottom(const _Tree &_tree, std::size_t _bp, std::size_t _ep, _Report _report)
-> std::pair<decltype(_tree.Position(1)), decltype(_tree.Position(1))> {
  auto l = _tree.Leaf(_bp);
  if (_tree.Position(l) < _bp) ++l;

  auto r = _tree.Leaf(_ep) - 1;

  for (auto i = l, next = i + 1; i <= r; i = next, ++next) {
    while (_tree.IsFirstChild(i)) {
      auto p = _tree.Parent(i);
      if (p.second > r + 1)
        break;
      i = p.first;
      next = p.second;
    }

    _report(i);
  }

  return std::make_pair(_tree.Position(l), _tree.Position(r + 1));
}


template<typename _Tree>
class ComputeCoverBottomFunctor {
 public:
  explicit ComputeCoverBottomFunctor(const _Tree &_tree) : tree_(_tree) {}

  auto Compute(std::size_t _bp, std::size_t _ep) const {
    std::vector<std::size_t> nodes;

    auto report = [&nodes](const auto &_value) { nodes.emplace_back(_value); };

    auto range = ComputeCoverFromBottom(tree_, _bp, _ep, report);

    return std::make_pair(std::move(range), std::move(nodes));
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


/**
 * Expand the segment [_sp, _ep) correspond to the SLP.
 *
 * The function computes the maximal nodes of the parse tree of the SLP that cover the segment [_sp, _ep). Then it
 * concatenates the expansion of each node in the cover.
 *
 * @tparam _SLP
 * @tparam _Report
 *
 * @param _slp
 * @param _bp
 * @param _ep
 * @param _report
 */
template<typename _SLP, typename _Report>
void ExpandSLPCover(const _SLP &_slp, std::size_t _bp, std::size_t _ep, _Report &_report) {
  if (_bp >= _ep)
    return;

  auto leaf = _slp.Leaf(_bp);
  auto pos = _slp.Position(leaf);

  while (_bp < _ep) {
    std::vector<uint32_t> vars;
    grammar::ComputeSpanCover(_slp, _bp - pos, _ep - pos, back_inserter(vars), _slp.Map(leaf));

    for (const auto &v : vars) {
      auto span = _slp.Span(v);
      std::for_each(span.begin(), span.end(), _report);
    }

    _bp = pos = _slp.Position(++leaf);
  }
}


template<typename _SLP>
class ExpandSLPCoverFunctor {
 public:
  explicit ExpandSLPCoverFunctor(_SLP &_slp) : slp_{_slp} {}

  template<typename _Report>
  void operator()(std::size_t _bp, std::size_t _ep, _Report &_report) const {
    ExpandSLPCover(slp_, _bp, _ep, _report);
  }

 private:
  const _SLP &slp_;
};


class SLPGetSpan {
 public:
  template<typename _Result, typename _SA, typename _SLP, typename _PTS>
  void operator()(std::size_t _bp,
                  std::size_t _ep,
                  _Result &_result,
                  const _SA &_sa,
                  const _SLP &_slp,
                  const _PTS &_pts) const {
    auto report = [&_result](auto &&_value) { _result.emplace_back(_value); };

    ExpandSLPCover(_slp, _bp, _ep, report);
  }
};


template<typename _VarType, typename _SLP, typename _Report>
void ExpandSLPFromLeft(_VarType _var, std::size_t &_length, const _SLP &_slp, _Report &_report) {
  if (_slp.IsTerminal(_var)) {
    _report(_var);
    --_length;
    return;
  }

  const auto &children = _slp[_var];
  ExpandSLPFromLeft(children.first, _length, _slp, _report);

  if (_length) {
    ExpandSLPFromLeft(children.second, _length, _slp, _report);
  }
}


template<typename _VarType, typename _SLP, typename _Report>
void ExpandSLPFromFront(_VarType _var, std::size_t _length, const _SLP &_slp, _Report &_report) {
  const auto &cover = _slp.Cover(_var);

  auto size = cover.size();

  for (auto it = cover.begin(); _length && size; --size, ++it) {
    ExpandSLPFromLeft(*it, _length, _slp, _report);
  }
}


template<typename _VarType, typename _SLP, typename _Report>
void ExpandSLPFromLeft(_VarType _var, std::size_t &_skip, std::size_t &_length, const _SLP &_slp, _Report &_report) {
  if (_slp.IsTerminal(_var)) {
    --_skip;
    return;
  }

  const auto &children = _slp[_var];
  ExpandSLPFromLeft(children.first, _skip, _length, _slp, _report);

  if (_skip) {
    ExpandSLPFromLeft(children.second, _skip, _length, _slp, _report);
  } else if (_length) {
    ExpandSLPFromLeft(children.second, _length, _slp, _report);
  }
}


template<typename _VarType, typename _SLP, typename _Report>
void ExpandSLPFromFront(_VarType _var, std::size_t _skip, std::size_t _length, const _SLP &_slp, _Report &_report) {
  const auto &cover = _slp.Cover(_var);

  auto size = cover.size();

  for (auto it = cover.begin(); _length && size; --size, ++it) {
    if (_skip) {
      ExpandSLPFromLeft(*it, _skip, _length, _slp, _report);
    } else {
      ExpandSLPFromLeft(*it, _length, _slp, _report);
    }
  }
}


template<typename _VarType, typename _SLP, typename _Report>
void ExpandSLPFromRight(_VarType _var, std::size_t &_length, const _SLP &_slp, _Report &_report) {
  if (_slp.IsTerminal(_var)) {
    _report(_var);
    --_length;
    return;
  }

  const auto &children = _slp[_var];
  ExpandSLPFromRight(children.second, _length, _slp, _report);

  if (_length) {
    ExpandSLPFromRight(children.first, _length, _slp, _report);
  }
}


template<typename _VarType, typename _SLP, typename _Report>
void ExpandSLPFromBack(_VarType _var, std::size_t _length, const _SLP &_slp, _Report &_report) {
  const auto &cover = _slp.Cover(_var);

  auto size = cover.size();

  for (auto it = cover.end() - 1; _length && size; --size, --it) {
    ExpandSLPFromRight(*it, _length, _slp, _report);
  }
}


template<typename _VarType, typename _SLP, typename _Report>
void ExpandSLPFromRight(_VarType _var, std::size_t &_skip, std::size_t &_length, const _SLP &_slp, _Report &_report) {
  if (_slp.IsTerminal(_var)) {
    --_skip;
    return;
  }

  const auto &children = _slp[_var];
  ExpandSLPFromRight(children.second, _skip, _length, _slp, _report);

  if (_skip) {
    ExpandSLPFromRight(children.first, _skip, _length, _slp, _report);
  } else if (_length) {
    ExpandSLPFromRight(children.first, _length, _slp, _report);
  }
}


template<typename _VarType, typename _SLP, typename _Report>
void ExpandSLPFromBack(_VarType _var, std::size_t _skip, std::size_t _length, const _SLP &_slp, _Report &_report) {
  const auto &cover = _slp.Cover(_var);

  auto size = cover.size();

  for (auto it = cover.end() - 1; _length && size; --size, --it) {
    if (_skip) {
      ExpandSLPFromRight(*it, _skip, _length, _slp, _report);
    } else {
      ExpandSLPFromRight(*it, _length, _slp, _report);
    }
  }
}


template<typename _SLP, typename _Report>
void ExpandSLP(const _SLP &_slp, std::size_t _bp, std::size_t _ep, _Report &_report) {
  if (_bp >= _ep)
    return;

  auto leaf = _slp.Leaf(_bp);
  auto pos = _slp.Position(leaf);

  if (pos == _bp) {
    ExpandSLPFromFront(leaf, _ep - pos, _slp, _report);
  } else {
    auto next_pos = _slp.Position(leaf + 1);

    if (next_pos <= _ep) {
      ExpandSLPFromBack(leaf, next_pos - _bp, _slp, _report);

      if (next_pos < _ep) {
        ExpandSLPFromFront(leaf + 1, _ep - next_pos, _slp, _report);
      }
    } else {
      auto skip_front = _bp - pos;
      auto skip_back = next_pos - _ep;
      if (skip_front < skip_back) {
        ExpandSLPFromFront(leaf, skip_front, _ep - _bp, _slp, _report);
      } else {
        ExpandSLPFromBack(leaf, skip_back, _ep - _bp, _slp, _report);
      }
    }
  }
}


template<typename _SLP>
class ExpandSLPFunctor {
 public:
  explicit ExpandSLPFunctor(_SLP &_slp) : slp_{_slp} {}

  template<typename _Report>
  void operator()(std::size_t _bp, std::size_t _ep, _Report &_report) const {
    ExpandSLP(slp_, _bp, _ep, _report);
  }

 private:
  const _SLP &slp_;
};


class LSLPGetSpan {
 public:
  template<typename _Result, typename _SA, typename _SLP, typename _PTS>
  void operator()(std::size_t _first,
                  std::size_t _last,
                  _Result &_result,
                  const _SA &_sa,
                  const _SLP &_slp,
                  const _PTS &_pts) const {
    auto report = [&_result](const auto &_value) {
      _result.emplace_back(_value);
    };

    ExpandSLP(_slp, _first, _last, report);
  }
};

}

#endif //DRL_INCLUDE_DRL_HELPER_H_
