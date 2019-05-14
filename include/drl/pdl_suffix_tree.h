//
// Created by Dustin Cobas Batista <dustin.cobas@gmail.com> on 02-05-19.
//

#ifndef DRL_PDL_SUFFIX_TREE_H
#define DRL_PDL_SUFFIX_TREE_H

#include "pdltree.h"


namespace drl {

template<typename _Report>
auto ComputeCoverSuffixTree(const PDLTree &tree, std::size_t _bp, std::size_t _ep, _Report _report) {
  pair_type sa_range = {_bp, _ep - 1};

  // Process the part of the range before the first full block.
  pair_type first_block = tree.getFirstBlock(sa_range);
//  if(first_block.first > sa_range.first)
//  {
//    usint temp = std::min(sa_range.second, first_block.first - 1);
//    bruteForceDocList(this->rlcsa, pair_type(sa_range.first, temp), result);
//    if(sa_range.second <= temp) { CSA::removeDuplicates(result, false); return result; }
//  }
  usint current = first_block.second; // Current block.

  // Process the part of the range after the last full block.
  pair_type last_block = tree.getLastBlock(sa_range);
//  if(last_block.second == this->tree->getNumberOfNodes()) // No block ends before the end of the range.
//  {
//    bruteForceDocList(this->rlcsa, pair_type(first_block.first, sa_range.second), result);
//    CSA::removeDuplicates(result, false);
//    return result;
//  }
//  if(last_block.first < sa_range.second)
//  {
//    bruteForceDocList(this->rlcsa, pair_type(last_block.first + 1, sa_range.second), result);
//  }
  usint limit = last_block.second + 1;  // First block not in the range.

  // Decompress the blocks.
//  usint run_start = 0, run_length = 0; // Current run of leaf blocks.
  while (current < limit) {
    pair_type ancestor = tree.getAncestor(current, limit);
    _report(ancestor.first);

    if (ancestor.first == current) {
      ++current;
    } else {
      current = ancestor.second;
    }

//    if(ancestor.first == current) // Leaf block.
//    {
//      if(run_length == 0) { run_start = current; run_length = 1; }
//      else { run_length++; }
//      current++;
//    }
//    else
//    {
//      if(run_length > 0)
//      {
//        //todo report {run_start, run_length}
////        if(this->addBlocks(run_start, run_length, result)) { return result; }
//        run_length = 0;
//      }
//      //todo report {ancestor.first, 1}
////      if(this->addBlocks(ancestor.first, 1, result)) { return result; }
//      current = ancestor.second;
//    }
  }
//  if(run_length > 0)
//  {
//    //todo report {run_start, run_length}
////    if(this->addBlocks(run_start, run_length, result)) { return result; }
//    run_length = 0;
//  }

//  CSA::removeDuplicates(result, false);

  return std::make_pair(first_block.first, last_block.first + 1);
}


template<typename _Tree>
class ComputeCoverSuffixTreeFunctor {
 public:
  explicit ComputeCoverSuffixTreeFunctor(const _Tree &_tree) : tree_(_tree) {}

  auto Compute(std::size_t _bp, std::size_t _ep) const {
    std::vector<std::size_t> nodes;
//    nodes.reserve((_ep - _bp));

    auto report = [&nodes](const auto &_value) { nodes.emplace_back(_value); };

    auto range = ComputeCoverSuffixTree(tree_, _bp, _ep, report);

    return std::make_pair(std::move(range), std::move(nodes));
  }

  auto operator()(std::size_t _sp, std::size_t _ep) const {
    return Compute(_sp, _ep);
  }

 protected:
  const _Tree &tree_;
};


template<typename _Tree>
auto BuildComputeCoverSuffixTreeFunctor(const _Tree &_tree) {
  return ComputeCoverSuffixTreeFunctor<_Tree>(_tree);
}


template<typename _Tree, typename _Blocks, typename _Grammar>
class GetDocsSuffixTreeRP {
 public:
  GetDocsSuffixTreeRP(const _Tree &_tree, const _Blocks &_blocks, const _Grammar &_grammar) : tree_{_tree},
                                                                                              blocks_{_blocks},
                                                                                              grammar_{_grammar} {}

  auto operator[](std::size_t _i) const {
    std::vector<uint32_t> set;
    auto report = [&set](const auto &_value) { set.emplace_back(_value); };

    addBlocks(_i, 1, report);

    return set;
  }

  template<typename _Report>
  void addBlocks(usint first_block, usint number_of_blocks, _Report &_report) const {
    CSA::MultiArray::Iterator *iter = blocks_.getIterator();
    iter->goToItem(first_block, 0);
    iter->setEnd(first_block + number_of_blocks, 0);

    std::stack<usint> buffer;
    while (!(iter->atEnd())) {
      buffer.push(iter->nextItem());
      while (!(buffer.empty())) {
        usint value = buffer.top();
        buffer.pop();
        if (tree_.isTerminal(value)) {
          if (value == tree_.getNumberOfDocuments()) {
            delete iter;
//            iter = 0;
//            allDocuments(this->tree->getNumberOfDocuments(), _report);
            for (std::size_t i = 0; i < tree_.getNumberOfDocuments(); ++i) {
              _report(i);
//              _report.emplace_back(i);
            }
            return;
          } else {
            _report(value);
//            _report.emplace_back(value);
          }
        } else {
          value = tree_.toRule(value);
          buffer.push(grammar_.readItemConst(2 * value + 1));
          buffer.push(grammar_.readItemConst(2 * value));
        }
      }
    }

    delete iter;
//    iter = 0;
  }

  template<typename _Report>
  void addBlocks1(usint first_block, usint number_of_blocks, _Report &_report) const {
    CSA::MultiArray::Iterator *iter = blocks_.getIterator();
    iter->goToItem(first_block, 0);
    iter->setEnd(first_block + number_of_blocks, 0);

    while (!(iter->atEnd())) {
      addBlocks2(iter->nextItem(), _report);
    }

    delete iter;
  }

  template<typename _Report>
  void addBlocks2(usint value, _Report &_report) const {
    if (tree_.isTerminal(value)) {
      if (value == tree_.getNumberOfDocuments()) {
        for (std::size_t i = 0; i < tree_.getNumberOfDocuments(); ++i) {
          _report(i);
//          _report.emplace_back(i);
        }
      } else {
        _report(value);
//        _report.emplace_back(value);
      }
      return;
    }
    value = tree_.toRule(value);
    addBlocks2(grammar_.readItemConst(2 * value + 1), _report);
    addBlocks2(grammar_.readItemConst(2 * value), _report);
  }

//  template<typename _Report>
//  void allDocuments(usint n, _Report _report) {
//    PDLRP::result_type buffer;
//    buffer.reserve(n);
//    for (usint i = 0; i < n; i++) { buffer.push_back(i); }
//    _report->swap(buffer);
//  }

 private:
  const _Tree &tree_;
  const _Blocks &blocks_;
  const _Grammar &grammar_;
};


template<typename _Tree, typename _Blocks, typename _Grammar>
auto BuildGetDocsSuffixTreeRP(const _Tree &_tree, const _Blocks &_blocks, const _Grammar &_grammar) {
  return GetDocsSuffixTreeRP<_Tree, _Blocks, _Grammar>(_tree, _blocks, _grammar);
}


template<typename _Tree>
class PDLBC {
 public:
  PDLBC(const _Tree &_tree, std::ifstream &_in, usint _flags) : tree_{_tree}, uses_rle{false} {
    if (_flags & RLE_FLAG) {
      uses_rle = true;
    }

    rule_borders = std::make_shared<CSA::SuccinctVector>(_in);
    rules = std::make_shared<CSA::ReadBuffer>(_in, rule_borders->getSize(), CSA::length(getNumberOfDocuments()));
    block_borders = std::make_shared<CSA::SuccinctVector>(_in);
    blocks = std::make_shared<CSA::ReadBuffer>(_in, block_borders->getSize(), CSA::length(maxInteger()));
    has_grammar = true;
  }

  template<class _Report>
  void addBlocks(usint first, usint number, _Report &_report) const {
    if (first >= this->getNumberOfNodes() || number == 0) { return; }

    CSA::SuccinctVector::Iterator iter(*(this->block_borders));
    usint from = iter.select(first);
    usint to = (number == 1 ? iter.selectNext() : iter.select(first + number));

    for (usint i = from; i < to; i++) {
      usint val = this->blocks->readItemConst(i);
      if (val >= this->maxInteger()) {
        addRun(0, getNumberOfDocuments(), _report);
        return;
      }  // Block contains all documents.
      if (val < this->getNumberOfDocuments()) // Value is a document id.
      {
        this->addItem(val, _report);
      } else  // Value is a rule id.
      {
        val -= this->getNumberOfDocuments();
        CSA::SuccinctVector::Iterator rule_iter(*(this->rule_borders));
        usint rfrom = rule_iter.select(val);
        usint rto = rule_iter.selectNext();

        if (this->usesRLE()) {
          usint run_start = 0;
          bool want_run_length = false;
          for (usint j = rfrom; j < rto; j++) {
            val = this->rules->readItemConst(j);
            if (val >= this->getNumberOfDocuments()) {
              addRun(0, getNumberOfDocuments(), _report);
              return;
            }  // Rule covers all documents.
            else if (want_run_length) {
              this->addRun(run_start, val, _report);
              want_run_length = false;
            } else {
              run_start = val;
              want_run_length = true;
            }
          }
        } else {
          for (usint j = rfrom; j < rto; j++) {
            val = this->rules->readItemConst(j);
            if (val >= this->getNumberOfDocuments()) {
              addRun(0, getNumberOfDocuments(), _report);
              return;
            }  // Rule covers all documents.
            else { this->addItem(val, _report); }
          }
        }
      }
    }
  }

  usint reportSize() const {
    usint bytes = sizeof(*this);

    usint rule_bytes = 0, block_bytes = 0;
    if (this->hasGrammar()) {
      rule_bytes = this->rule_borders->reportSize() + this->rules->reportSize();
      block_bytes = this->block_borders->reportSize() + this->blocks->reportSize();
    }

    bytes += rule_bytes + block_bytes;

    return bytes;
  }

 private:
  const _Tree &tree_;

  std::shared_ptr<CSA::SuccinctVector> rule_borders;
  std::shared_ptr<CSA::ReadBuffer> rules;        // this->getNumberOfDocs() means all documents.

  std::shared_ptr<CSA::SuccinctVector> block_borders;
  std::shared_ptr<CSA::ReadBuffer> blocks;       // Value this->maxInteger() means all documents.

  bool ok, has_grammar, uses_rle;

  const static usint RLE_FLAG = 0x01;

  inline bool isOk() const { return this->ok; }

  inline bool hasGrammar() const { return this->has_grammar; }

  inline bool usesRLE() const { return this->uses_rle; }

  inline usint getNumberOfNodes() const { return this->getNumberOfLeaves() + this->getNumberOfInternalNodes(); }

  inline usint getNumberOfLeaves() const { return tree_.getNumberOfLeaves(); }

  inline usint getNumberOfInternalNodes() const { return tree_.getNumberOfInternalNodes(); }

  inline usint maxInteger() const { return getNumberOfDocuments() + getNumberOfRules(); }

  inline usint getNumberOfDocuments() const { return tree_.getNumberOfDocuments(); }

  inline usint getNumberOfRules() const { return rule_borders->getNumberOfItems(); }

  template<typename _Report>
  inline void addRun(usint from, usint length, _Report &_report) const {
    for (usint i = from; i < from + length; i++) { _report(i); }
  }

  template<typename _Report>
  inline void addItem(usint item, _Report &_report) const {
    _report(item);
  }
};

}

#endif //DRL_PDL_SUFFIX_TREE_H
