#ifndef _DOCLIST_PDLTREE_H
#define _DOCLIST_PDLTREE_H

#include <cstdio>

#include <rlcsa/bits/multiarray.h>

#include "utils.h"

struct PDLTreeNode;

//--------------------------------------------------------------------------

class PDLTree
{
  public:
    const static std::string EXTENSION;
    const static std::string SETS_EXTENSION;
    const static std::string COUNT_EXTENSION;
    const static std::string FAST_COUNT_EXTENSION;

    typedef enum { mode_rp, mode_set, mode_topk, mode_merge, mode_count, mode_fc } mode_type;
    typedef enum { sort_none, sort_topk, sort_id } sort_type;

    PDLTree(const RLCSA& _rlcsa, usint _block_size, usint _storing_parameter, mode_type _mode, bool print = false);
    PDLTree(const RLCSA& _rlcsa, std::ifstream& input);
    PDLTree(const RLCSA& _rlcsa, FILE* input);
    ~PDLTree();

    void writeTo(std::ofstream& output) const;
    void writeTo(FILE* output) const;

    usint reportSize() const;

    inline bool isOk() const { return this->ok; }

    // Output can be std::ofstream or FILE.
    template <class Output> void writeSets(Output& output);

    // User must delete the returned array. These require the nodes.
    CSA::DeltaMultiArray* getFrequencies(bool differential_encoding);
    CSA::Array* getCounts();

    // Removes the explicit tree structure used during construction.
    void deleteNodes();

//--------------------------------------------------------------------------

    // Returns this->getNumberOfNodes(), if the range does not correspond to any block.
    usint getBlock(pair_type sa_range) const;

    // Finds the blocks covering sa_range. The result is empty if no such cover exists.
    void getBlocks(pair_type sa_range, std::vector<usint>& result) const;

    // Finds the first block starting after the start of sa_range.
    // Returns (starting_position, block_id).
    pair_type getFirstBlock(pair_type sa_range) const;

    // Finds the last block ending before the end of sa_range.
    // Returns (ending_position, block_id).
    // If no such block exists, the block_id is this->getNumberOfNodes().
    pair_type getLastBlock(pair_type sa_range) const;

    // Finds the first block starting strictly after the end of sa_range.
    // Returns (starting_position, block_id).
    pair_type getNextBlock(pair_type sa_range) const;

    // Finds the highest ancestor for which the block is the first leaf and the leaf following
    // the ancestor is at most next_leaf.
    // Returns (ancestor, leaf block following ancestor).
    pair_type getAncestor(usint block, usint next_leaf) const;

//--------------------------------------------------------------------------

    inline usint getNumberOfDocuments() const { return this->rlcsa.getNumberOfSequences(); }
    inline usint getNumberOfNodes() const { return this->getNumberOfLeaves() + this->getNumberOfInternalNodes(); }
    inline usint getNumberOfLeaves() const { return this->leaf_ranges->getNumberOfItems(); }
    inline usint getNumberOfInternalNodes() const { return this->first_children->getNumberOfItems(); }

    // Symbol is an integer in a file compressed by RePair.
    // Value is either a terminal or a grammar rule.
    inline bool isTerminal(usint symbol) const
    {
      return (symbol <= this->getNumberOfDocuments());
    }
    inline bool isEndMarker(usint symbol) const
    {
      return (symbol > this->getNumberOfDocuments()) &&
             (symbol <= this->getNumberOfDocuments() + this->getNumberOfNodes());
    }
    inline bool isNonTerminal(usint symbol) const
    {
      return (symbol > this->getNumberOfDocuments() + this->getNumberOfNodes());
    }
    inline usint toValue(usint nonterminal) const
    {
      return nonterminal - this->getNumberOfNodes();
    }
    inline usint isRule(usint value) const
    {
      return (value > this->getNumberOfDocuments());
    }
    inline usint toRule(usint value) const
    {
      return value - this->getNumberOfDocuments() - 1;
    }

//--------------------------------------------------------------------------

    // These are used to handle different modes.

    inline bool useContainsAll() const
    {
      return (this->mode == mode_rp ||
              this->mode == mode_set ||
              this->mode == mode_count ||
              this->mode == mode_fc);
    }

    inline bool storeAllInternalNodes() const
    {
      return (this->mode == mode_topk || this->mode == mode_count || this->mode == mode_fc);
    }

    inline bool canRemoveSets() const { return (this->mode == mode_count || this->mode == mode_fc); }
    inline bool buildFullTree() const { return (this->mode == mode_fc); }

    inline bool usesFrequencies() const { return (this->mode == mode_topk || this->mode == mode_merge); }
    inline bool usesCounts() const { return (this->mode == mode_count || this->mode == mode_fc); }

    inline bool usesSets() const
    {
      return (this->mode == mode_rp ||
              this->mode == mode_set ||
              this->mode == mode_topk ||
              this->mode == mode_merge);
    }

    inline bool usesRPSet() const { return (this->mode == mode_set); }

    inline sort_type getSortOrder() const
    {
      if(this->mode == mode_topk || this->mode == mode_merge) { return sort_topk; }
      return sort_id;
    }

    // Returns the proper extension for the PDLTree file, depending on mode.
    std::string getExtension() const;

//--------------------------------------------------------------------------

  private:
    const static usint RANGE_BLOCK_SIZE = 32;
    const static usint PARENT_BLOCK_SIZE = 32;
    const static usint FREQ_BLOCK_SIZE = 32;
    const static usint COUNT_BLOCK_SIZE = 32;

    const RLCSA& rlcsa;
    usint block_size, storing_parameter;

    bool ok;
    mode_type mode; // Only meaningful during construction.
    sort_type sort_order;

    // These form the actual tree structure.
    CSA::DeltaVector*    leaf_ranges;
    CSA::SuccinctVector* first_children;
    CSA::ReadBuffer*     parents;
    CSA::ReadBuffer*     next_leaves;

    // These are used during construction.
    PDLTreeNode* root;
    PDLTreeNode** getNodesInOrder(sort_type _sort_order);

    // These are used during queries.
    pair_type getFirstBlock(pair_type sa_range, CSA::DeltaVector::Iterator& iter) const;
    pair_type getNextBlock(pair_type sa_range, CSA::DeltaVector::Iterator& iter) const;
    pair_type parentOf(usint tree_node, CSA::SuccinctVector::Iterator& iter) const;

    // These are not allowed.
    PDLTree();
    PDLTree(const PDLTree&);
    PDLTree& operator = (const PDLTree&);
};

// After the call of readGrammar, terminals contains a value that should be passed to readBlocks.
CSA::ReadBuffer* readGrammar(const PDLTree& tree, const std::string& base_name, uint& terminals);
CSA::MultiArray* readBlocks(const PDLTree& tree, const std::string& base_name, uint terminals);

//--------------------------------------------------------------------------

struct PDLTreeNode
{
  uint         string_depth;
  pair_type    range;
  PDLTreeNode* parent;
  PDLTreeNode* child;
  PDLTreeNode* sibling;
  PDLTreeNode* next;

  usint        stored_documents;
  uint         id;
  bool         contains_all;

  std::vector<Document>* docs;

  PDLTreeNode(uint lcp, pair_type sa_range);
  ~PDLTreeNode();

  void addChild(PDLTreeNode* node);
  void addSibling(PDLTreeNode* node);

  void deleteChildren();

  void addLeaves();   // Adds leaves to the sparse suffix tree when required.
  PDLTreeNode* addLeaf(PDLTreeNode* left, PDLTreeNode* right, usint pos);

  bool verifyTree();  // Call this to ensure that the tree is correct.

  // Removes this node from the tree, replacing it by its children.
  void remove();

  void determineSize(uint& nodes, uint& leaves);
  void computeStoredDocuments(usint* documents);
  void containsAllDocuments(bool delete_set);  // Use this to tell that the node contains all documents.

  // Use this to tell that the set contained in the node will be stored.
  void storeThisSet(bool remove_child_sets);

  void setNext();

  // For PDL-fast.
  // Returns 0, if occ > docc for a child of the node.
  // Otherwise returns the number of children.
  usint canDeleteChildren();

  const static usint PARALLEL_SORT_THRESHOLD = 32768;
};

//--------------------------------------------------------------------------

inline void
writeInteger(uint val, std::ofstream& output)
{
  output.write((char*)&val, sizeof(val));
}

inline void
writeInteger(uint val, FILE& output)
{
  fwrite((void*)&val, sizeof(val), 1, &output);
}

template<class Output>
void
PDLTree::writeSets(Output& output)
{
  if(!(this->isOk()) || this->root == 0) { return; }

  if(this->usesRPSet()) { writeInteger(this->getNumberOfDocuments(), output); }

  PDLTreeNode** order = this->getNodesInOrder(this->getSortOrder());
  for(usint i = 0; i < this->getNumberOfNodes(); i++)
  {
    if(order[i]->docs != 0)
    {
      for(std::vector<Document>::iterator iter = order[i]->docs->begin(); iter != order[i]->docs->end(); ++iter)
      {
        writeInteger(iter->first, output);
      }
    }
    else
    {
      writeInteger(this->getNumberOfDocuments(), output); // A symbol expanding to all document identifiers.
    }
    writeInteger(this->getNumberOfDocuments() + 1 + i, output); // Unique endmarker for set i.
  }
  delete[] order; order = 0;
}

//--------------------------------------------------------------------------

#endif  // _DOCLIST_PDLTREE_H
