#include <stack>

#include "drl/pdltree.h"

//--------------------------------------------------------------------------

const std::string PDLTree::EXTENSION = ".pdltree";
const std::string PDLTree::SETS_EXTENSION = ".sets";
const std::string PDLTree::COUNT_EXTENSION = ".pdlcount";
const std::string PDLTree::FAST_COUNT_EXTENSION = ".pdlfc";

std::ostream&
operator<<(std::ostream& stream, const PDLTreeNode& data)
{
  stream << "(depth = " << data.string_depth << ", range = (" << data.range.first << ", " << data.range.second << "), parent = " << data.parent;

  stream << ", siblings = (";
  for(PDLTreeNode* curr = data.sibling; curr != 0; curr = curr->sibling)
  {
    if(curr != data.sibling) { stream << ", "; }
    stream << curr;
  }
  stream << ")";

  stream << ", children = (";
  for(PDLTreeNode* curr = data.child; curr != 0; curr = curr->sibling)
  {
    if(curr != data.child) { stream << ", "; }
    stream << curr;
  }
  stream << ")";

  stream << ")";
  return stream;
}

PDLTreeNode::PDLTreeNode(uint lcp, pair_type sa_range) :
  string_depth(lcp), range(sa_range),
  parent(0), child(0), sibling(0), next(0),
  stored_documents(0), id(0), contains_all(false),
  docs(0)
{
}

PDLTreeNode::~PDLTreeNode()
{
  this->parent = 0;
  delete this->docs; this->docs = 0;
  delete this->child; this->child = 0;
  while(this->sibling != 0)
  {
    PDLTreeNode* temp = this->sibling; this->sibling = this->sibling->sibling;
    temp->sibling = 0; delete temp;
  }
}

void
PDLTreeNode::addChild(PDLTreeNode* node)
{
  node->parent = this;
  if(this->child == 0) { this->child = node; return; }
  this->child->addSibling(node);
}

void
PDLTreeNode::addSibling(PDLTreeNode* node)
{
  if(this->sibling == 0) { this->sibling = node; return; }
  this->sibling->addSibling(node);
}

void
PDLTreeNode::deleteChildren()
{
  delete this->child; this->child = 0;
}

void
PDLTreeNode::addLeaves()
{
  std::stack<PDLTreeNode*> nodestack;
  nodestack.push(this);

  while(!(nodestack.empty()))
  {
    PDLTreeNode* curr = nodestack.top(); nodestack.pop();
    if(curr->child == 0) { continue; }
    usint expect = curr->range.first;
    PDLTreeNode* prev = 0;
    for(PDLTreeNode* temp = curr->child; temp != 0; temp = temp->sibling)
    {
      while(expect < temp->range.first) { prev = curr->addLeaf(prev, temp, expect); expect++; }
      nodestack.push(temp);
      expect = temp->range.second + 1;
      prev = temp;
    }
    while(expect < curr->range.second + 1) { prev = curr->addLeaf(prev, 0, expect); expect++; }
  }
}

PDLTreeNode*
PDLTreeNode::addLeaf(PDLTreeNode* left, PDLTreeNode* right, usint pos)
{
  PDLTreeNode* leaf = new PDLTreeNode(0, pair_type(pos, pos));
  leaf->parent = this;
  if(left == 0) { this->child = leaf; }
  else          { left->sibling = leaf; }
  leaf->sibling = right;
  return leaf;
}

bool
PDLTreeNode::verifyTree()
{
  std::stack<PDLTreeNode*> nodestack;
  nodestack.push(this);

  while(!(nodestack.empty()))
  {
    PDLTreeNode* curr = nodestack.top(); nodestack.pop();
    if(curr->child == 0) { continue; }
    usint expect = curr->range.first, last = 0;
    for(PDLTreeNode* temp = curr->child; temp != 0; temp = temp->sibling)
    {
      if(temp->range.first != expect)
      {
        std::cerr << "Error: node " << *curr << ", expected " << expect << ", got " << *temp << std::endl;
        std::cerr << "Children:";
        for(temp = curr->child; temp != 0; temp = temp->sibling) { std::cerr << " " << *temp; }
        std::cerr << std::endl;
        if(curr->parent) { std::cerr << "Parent: " << *(curr->parent) << std::endl; }
        return false;
      }
      nodestack.push(temp);
      expect = temp->range.second + 1;
      last = temp->range.second;
    }
    if(last != curr->range.second)
    {
      std::cerr << "Error: node " << *curr << ", children end at " << last << std::endl;
      return false;
    }
  }

  return true;
}

void
PDLTreeNode::remove()
{
  if(this->child == 0 || this->parent == 0) { return; }  // Cannot remove root or leaves.

  PDLTreeNode* curr;
  for(curr = this->child; curr != 0; curr = curr->sibling) { curr->parent = this->parent; }
  if(this == this->parent->child) { this->parent->child = this->child; }
  else
  {
    for(curr = this->parent->child; curr->sibling != this; curr = curr->sibling);
    curr->sibling = this->child;
  }
  for(curr = this->child; curr->sibling != 0; curr = curr->sibling);
  curr->sibling = this->sibling;

  this->parent = this->child = this->sibling = 0;
}

void
PDLTreeNode::determineSize(uint& nodes, uint& leaves)
{
  nodes = 0; leaves = 0;
  std::stack<PDLTreeNode*> nodestack;
  nodestack.push(this);

  while(!(nodestack.empty()))
  {
    PDLTreeNode* curr = nodestack.top(); nodestack.pop(); nodes++;
    if(curr->child == 0) { leaves++; }
    for(PDLTreeNode* temp = curr->child; temp != 0; temp = temp->sibling) { nodestack.push(temp); }
  }
}

void
mergeDuplicates(std::vector<Document>* docs)
{
  if(docs == 0) { return; }
  if(docs->size() >= PDLTreeNode::PARALLEL_SORT_THRESHOLD) { CSA::parallelSort(docs->begin(), docs->end()); }
  else { CSA::sequentialSort(docs->begin(), docs->end()); }
  mergeDocumentRuns(docs);
}

void
PDLTreeNode::computeStoredDocuments(usint* documents)
{
  this->stored_documents = 0;
  delete this->docs; this->docs = 0;
  if(this->child == 0)
  {
    this->docs = new std::vector<Document>; this->docs->reserve(CSA::length(this->range));
    for(usint i = this->range.first; i <= this->range.second; i++)
    {
      this->docs->push_back(Document(documents[i], 1));
    }
  }
  else
  {
    this->docs = new std::vector<Document>;
    for(PDLTreeNode* curr = this->child; curr != 0; curr = curr->sibling)
    {
      if(curr->contains_all && curr->docs == 0) // Contains all docs; document set not stored.
      {
        this->containsAllDocuments(true);
        this->stored_documents = curr->stored_documents;
        break;
      }
      this->docs->insert(this->docs->end(), curr->docs->begin(), curr->docs->end());
      this->stored_documents += curr->stored_documents;
    }
  }
  mergeDuplicates(this->docs);
}

void
PDLTreeNode::containsAllDocuments(bool delete_set)
{
  this->contains_all = true;
  if(delete_set)
  {
    this->stored_documents = this->docs->size();
    delete this->docs; this->docs = 0;
  }
}

void
PDLTreeNode::storeThisSet(bool delete_child_sets)
{
  if(this->docs != 0) { this->stored_documents = this->docs->size(); }

  if(delete_child_sets)
  {
    for(PDLTreeNode* curr = this->child; curr != 0; curr = curr->sibling)
    {
      delete curr->docs; curr->docs = 0;
    }
  }
}

void
PDLTreeNode::setNext()
{
  std::stack<PDLTreeNode*> nodestack;
  std::vector<PDLTreeNode*> previous;
  for(PDLTreeNode* temp = this; temp != 0; temp = temp->child) { nodestack.push(temp); }
  this->next = 0;

  while(nodestack.top() != this)
  {
    PDLTreeNode* curr = nodestack.top(); nodestack.pop();
    for(PDLTreeNode* temp = curr->sibling; temp != 0; temp = temp->child) { nodestack.push(temp); }

    if(curr->child == 0)
    {
      for(std::vector<PDLTreeNode*>::iterator iter = previous.begin(); iter != previous.end(); ++iter)
      {
        (*iter)->next = curr;
      }
      previous.clear();
    }
    curr->next = 0; previous.push_back(curr);
  }
}

usint
PDLTreeNode::canDeleteChildren()
{
  usint res = 0;
  for(PDLTreeNode* curr = this->child; curr != 0; curr = curr->sibling)
  {
    if(CSA::length(curr->range) > curr->stored_documents) { return 0; }
    res++;
  }
  return res;
}

//--------------------------------------------------------------------------

PDLTree::PDLTree(const RLCSA& _rlcsa, usint _block_size, usint _storing_parameter, mode_type _mode, bool print) :
  rlcsa(_rlcsa), block_size(_block_size), storing_parameter(_storing_parameter),
  ok(false), mode(_mode), sort_order(sort_none),
  leaf_ranges(0), first_children(0), parents(0), next_leaves(0),
  root(0)
{
  if(!(this->rlcsa.isOk()) || !(this->rlcsa.supportsLocate()))
  {
    std::cerr << "PDLTree::PDLTree(): Invalid RLCSA!" << std::endl;
    return;
  }
  if(this->buildFullTree()) { this->block_size = 2; }
  if(this->block_size == 0)
  {
    std::cerr << "PDLTree::PDLTree(): Trying to use block size 0!" << std::endl;
    return;
  }

  // Build SA.
  if(print) { std::cout << "Building SA..." << std::endl; }
  usint size = this->rlcsa.getSize();
  uint docs = this->rlcsa.getNumberOfSequences();
  usint* documents = new usint[size];
  #pragma omp parallel for schedule(static)
  for(usint i = 0; i < size; i += CSA::MEGABYTE)
  {
    this->rlcsa.locate(pair_type(i, std::min(i + CSA::MEGABYTE - 1, size - 1)), documents + i);
  }

  // Build LCP.
  if(print) { std::cout << "Building LCP..." << std::endl; }
  uint* lcp = new uint[size + 1]; lcp[size] = 0;
  CSA::PLCPVector* plcpvec = this->rlcsa.buildPLCP(16);
  CSA::PLCPVector::Iterator iter(*plcpvec);
  for(usint i = 0; i < size; i++)
  {
    lcp[i] = iter.select(documents[i]) - 2 * documents[i];
  }
  delete plcpvec; plcpvec = 0;

  // Convert SA to DA.
  if(print) { std::cout << "Building DA..." << std::endl; }
  #pragma omp parallel for schedule(static)
  for(usint i = 0; i < size; i += CSA::MEGABYTE)
  {
    this->rlcsa.getSequenceForPosition(documents + i, std::min(CSA::MEGABYTE, size - i));
  }

  // Build ST. Kind of.
  if(print) { std::cout << "Building ST..." << std::endl; }
  std::stack<PDLTreeNode*> nodestack; nodestack.push(new PDLTreeNode(0, pair_type(0, 0)));
  PDLTreeNode* prev = 0;
  for(usint i = 1; i <= size; i++)
  {
    usint left = i - 1;
    while(lcp[i] < nodestack.top()->string_depth)
    {
      nodestack.top()->range.second = i - 1;
      prev = nodestack.top(); nodestack.pop();
      if(CSA::length(prev->range) <= this->block_size) { prev->deleteChildren(); }
      this->root = prev;  // Last processed node.
      left = prev->range.first;
      if(lcp[i] <= nodestack.top()->string_depth) { nodestack.top()->addChild(prev); prev = 0; }
    }
    if(lcp[i] > nodestack.top()->string_depth)
    {
      PDLTreeNode* curr = new PDLTreeNode(lcp[i], pair_type(left, left));
      if(prev != 0) { curr->addChild(prev); prev = 0; }
      nodestack.push(curr);
    }
  }
  while(this->root->parent != 0) { this->root = this->root->parent; }  // Find the actual root.
  root->range.second = size - 1; // root->containsAllDocuments(this->contains_all);
  delete[] lcp; lcp = 0;
  root->addLeaves();
  root->verifyTree();

  // Build the sets of document ids.
  if(print) { std::cout << "Building sets..." << std::endl; }
  uint tree_leaves = 0, tree_nodes = 0;
  while(!nodestack.empty()) { nodestack.pop(); }
  for(prev = this->root; prev != 0; prev = prev->child) { nodestack.push(prev); }
  while(!(nodestack.empty()))
  {
    PDLTreeNode* curr = nodestack.top(); nodestack.pop();
    for(PDLTreeNode* temp = curr->sibling; temp != 0; temp = temp->child) { nodestack.push(temp); }

    curr->computeStoredDocuments(documents);
    if(curr->docs != 0 && curr->docs->size() >= docs) { curr->containsAllDocuments(this->useContainsAll()); }
    if(this->mode == mode_fc) // FIXME should not have raw mode checks
    {
      // temp > 0 means that all children of curr are leaves with occ == docc.
      usint temp = curr->canDeleteChildren();
      if(temp > 0) { curr->deleteChildren(); tree_leaves -= temp; }
    }

    if(this->storeAllInternalNodes())
    {
      curr->storeThisSet(this->canRemoveSets());
      if(curr->child == 0) { tree_leaves++; }
      else                 { tree_nodes++; }
    }
    else
    {
      if(curr->child == 0 || curr->contains_all || curr->stored_documents > storing_parameter * curr->docs->size())
      {
        curr->storeThisSet(this->canRemoveSets());
        if(curr->child == 0) { tree_leaves++; }
        else                 { tree_nodes++; }
      }
      else
      {
        curr->remove();
        delete curr; curr = 0;
      }
    }
  }
  root->setNext();

  // Set node identifiers.
  if(print)
  {
    std::cout << "Setting node identifiers (" << tree_leaves << " leaves, " << tree_nodes << " internal nodes)..." << std::endl;
  }
  tree_nodes = tree_leaves; tree_leaves = 0;
  while(!nodestack.empty()) { nodestack.pop(); }
  for(PDLTreeNode* curr = root; curr != 0; curr = curr->child) { nodestack.push(curr); }
  while(!(nodestack.empty()))
  {
    PDLTreeNode* curr = nodestack.top(); nodestack.pop();
    for(PDLTreeNode* temp = curr->sibling; temp != 0; temp = temp->child) { nodestack.push(temp); }
    if(curr->child == 0) { curr->id = tree_leaves; tree_leaves++; }
    else                 { curr->id = tree_nodes; tree_nodes++; }
  }

  // Determine the SA ranges corresponding to the leaves and mark the first children.
  if(print) { std::cout << "Building tree..." << std::endl; }
  CSA::DeltaVector::Encoder range_encoder(RANGE_BLOCK_SIZE);
  CSA::SuccinctVector::Encoder child_encoder(PARENT_BLOCK_SIZE,
    CSA::nextMultipleOf(PARENT_BLOCK_SIZE, BITS_TO_BYTES(tree_nodes)));
  while(!nodestack.empty()) { nodestack.pop(); }
  for(PDLTreeNode* curr = root; curr != 0; curr = curr->child) { nodestack.push(curr); }
  while(!(nodestack.empty()))
  {
    PDLTreeNode* curr = nodestack.top(); nodestack.pop();
    for(PDLTreeNode* temp = curr->sibling; temp != 0; temp = temp->child) { nodestack.push(temp); }
    if(curr->child == 0) { range_encoder.addBit(curr->range.first); }
    if(curr->parent != 0 && curr == curr->parent->child) { child_encoder.setBit(curr->id); }
  }
  this->leaf_ranges = new CSA::DeltaVector(range_encoder, this->root->range.second + 1);
  this->first_children = new CSA::SuccinctVector(child_encoder, tree_nodes);

  // Pointers to the parents for first children and to the next leaf for internal nodes.
  CSA::WriteBuffer par_buffer(tree_nodes - tree_leaves, CSA::length(tree_nodes - tree_leaves - 1));
  CSA::WriteBuffer next_buffer(tree_nodes - tree_leaves, CSA::length(tree_leaves));
  CSA::SuccinctVector::Iterator child_iter(*(this->first_children));
  while(!(nodestack.empty())) { nodestack.pop(); }
  for(PDLTreeNode* curr = this->root; curr != 0; curr = curr->child) { nodestack.push(curr); }
  while(!(nodestack.empty()))
  {
    PDLTreeNode* curr = nodestack.top(); nodestack.pop();
    for(PDLTreeNode* temp = curr->sibling; temp != 0; temp = temp->child) { nodestack.push(temp); }
    if(child_iter.isSet(curr->id))
    {
      par_buffer.goToItem(child_iter.rank(curr->id) - 1);
      par_buffer.writeItem(curr->parent->id - tree_leaves);
    }
    if(curr->id >= tree_leaves)
    {
      next_buffer.writeItem(curr->next == 0 ? tree_leaves : curr->next->id);
    }
  }
  this->parents = par_buffer.getReadBuffer();
  this->next_leaves = next_buffer.getReadBuffer();

  this->ok = true;
}

PDLTree::PDLTree(const RLCSA& _rlcsa, std::ifstream& input) :
  rlcsa(_rlcsa), block_size(0), storing_parameter(0),
  ok(false), mode(mode_rp), sort_order(sort_none),
  leaf_ranges(0), first_children(0), parents(0), next_leaves(0),
  root(0)
{
  this->leaf_ranges = new CSA::DeltaVector(input);
  this->first_children = new CSA::SuccinctVector(input);
  this->parents = new CSA::ReadBuffer(input, this->getNumberOfInternalNodes(),
    CSA::length(this->getNumberOfInternalNodes() - 1));
  this->next_leaves = new CSA::ReadBuffer(input, this->getNumberOfInternalNodes(),
    CSA::length(this->getNumberOfLeaves()));
  this->ok = true;
}

PDLTree::PDLTree(const RLCSA& _rlcsa, FILE* input) :
  rlcsa(_rlcsa), block_size(0), storing_parameter(0),
  ok(false), mode(mode_rp), sort_order(sort_none),
  leaf_ranges(0), first_children(0), parents(0), next_leaves(0),
  root(0)
{
  this->leaf_ranges = new CSA::DeltaVector(input);
  this->first_children = new CSA::SuccinctVector(input);
  this->parents = new CSA::ReadBuffer(input, this->getNumberOfInternalNodes(),
    CSA::length(this->getNumberOfInternalNodes() - 1));
  this->next_leaves = new CSA::ReadBuffer(input, this->getNumberOfInternalNodes(),
    CSA::length(this->getNumberOfLeaves()));
  this->ok = true;
}

PDLTree::~PDLTree()
{
  delete this->leaf_ranges; this->leaf_ranges = 0;
  delete this->first_children; this->first_children = 0;
  delete this->parents; this->parents = 0;
  delete this->next_leaves; this->next_leaves = 0;
  delete this->root; this->root = 0;
}

void
PDLTree::writeTo(std::ofstream& output) const
{
  this->leaf_ranges->writeTo(output);
  this->first_children->writeTo(output);
  this->parents->writeBuffer(output);
  this->next_leaves->writeBuffer(output);
}

void
PDLTree::writeTo(FILE* output) const
{
  this->leaf_ranges->writeTo(output);
  this->first_children->writeTo(output);
  this->parents->writeBuffer(output);
  this->next_leaves->writeBuffer(output);
}

std::string
PDLTree::getExtension() const
{
  if(this->mode == mode_count) { return COUNT_EXTENSION; }
  else if(this->mode == mode_fc) { return FAST_COUNT_EXTENSION; }
  else { return EXTENSION; }
}

usint
PDLTree::reportSize() const
{
  usint bytes = sizeof(*this);
  if(this->leaf_ranges != 0) { bytes += this->leaf_ranges->reportSize(); }
  if(this->first_children != 0) { bytes += this->first_children->reportSize(); }
  if(this->parents != 0) { bytes += this->parents->reportSize(); }
  if(this->next_leaves != 0) { bytes += this->next_leaves->reportSize(); }
  return bytes;
}

//--------------------------------------------------------------------------

PDLTreeNode**
PDLTree::getNodesInOrder(sort_type _sort_order)
{
  PDLTreeNode** order = new PDLTreeNode*[this->getNumberOfNodes()];
  std::stack<PDLTreeNode*> nodestack;
  for(PDLTreeNode* curr = this->root; curr != 0; curr = curr->child) { nodestack.push(curr); }
  while(!(nodestack.empty()))
  {
    PDLTreeNode* curr = nodestack.top(); nodestack.pop();
    for(PDLTreeNode* temp = curr->sibling; temp != 0; temp = temp->child) { nodestack.push(temp); }
    order[curr->id] = curr;
  }

  if(this->sort_order != _sort_order)
  {
    #pragma omp parallel for schedule(dynamic, 1)
    for(usint i = 0; i < this->getNumberOfNodes(); i++)
    {
      PDLTreeNode* curr = order[i];
      if(curr->docs != 0)
      {
        if(_sort_order == sort_topk) { sortByFrequency(curr->docs); }
        else if(_sort_order == sort_id) { sortById(curr->docs); }
      }
    }
    this->sort_order = _sort_order;
  }

  return order;
}

CSA::DeltaMultiArray*
PDLTree::getFrequencies(bool differential_encoding)
{
  if(!(this->isOk()) || this->root == 0) { return 0; }
  if(this->useContainsAll())
  {
    std::cerr << "PDLTree::getFrequencies(): Frequencies are not stored, as CONTAINS_ALL symbol is in use!" << std::endl;
    return 0;
  }

  CSA::DeltaMultiArray* freqs = new CSA::DeltaMultiArray(FREQ_BLOCK_SIZE);
  PDLTreeNode** order = this->getNodesInOrder(sort_topk);
  for(usint i = 0; i < this->getNumberOfNodes(); i++)
  {
    freqs->nextArray();
    usint prev = CSA::WORD_MAX, diff = 0, run = 0;
    for(std::vector<Document>::iterator iter = order[i]->docs->begin(); iter != order[i]->docs->end(); ++iter)
    {
      if(iter->second == prev) { run++; continue; }
      if(run > 0) { freqs->writeItem(diff); freqs->writeItem(run); }
      diff = (prev == CSA::WORD_MAX || !differential_encoding ? iter->second : prev - iter->second);
      prev = iter->second; run = 1;
    }
    if(run > 0) { freqs->writeItem(diff); freqs->writeItem(run); }
  }
  delete[] order; order = 0;

  freqs->finishWriting();
  return freqs;
}

CSA::Array*
PDLTree::getCounts()
{
  if(!(this->isOk()) || this->root == 0)
  {
    std::cerr << "PDLTree::getCounts(): The tree does not contain document counts!" << std::endl;
    return 0;
  }

  CSA::Array::Encoder encoder(COUNT_BLOCK_SIZE);
  PDLTreeNode** order = this->getNodesInOrder(sort_id);
  for(usint i = 0; i < this->getNumberOfNodes(); i++)
  {
    encoder.writeItem(order[i]->stored_documents);
  }
  delete[] order; order = 0;

  return new CSA::Array(encoder);
}

void
PDLTree::deleteNodes()
{
  delete this->root; this->root = 0;
}

//--------------------------------------------------------------------------

usint
PDLTree::getBlock(pair_type sa_range) const
{
  // Check whether range.first starts a new block.
  CSA::DeltaVector::Iterator block_iter(*(this->leaf_ranges));
  pair_type first_block = this->getFirstBlock(sa_range, block_iter);
  if(first_block.first > sa_range.first)
  {
    return this->getNumberOfNodes();
  }

  // Check whether range.second ends a full block.
  pair_type next_block = this->getNextBlock(sa_range, block_iter);
  if(next_block.first != sa_range.second + 1)
  {
    return this->getNumberOfNodes();
  }

  // Check whether there is an ancestor that covers the exact range of blocks.
  pair_type ancestor = this->getAncestor(first_block.second, next_block.second);
  return (ancestor.second == next_block.second ? ancestor.first : this->getNumberOfNodes());
}

void
PDLTree::getBlocks(pair_type sa_range, std::vector<usint>& result) const
{
  result.empty();

  // Check whether range.first starts a new block.
  CSA::DeltaVector::Iterator block_iter(*(this->leaf_ranges));
  pair_type first_block = this->getFirstBlock(sa_range, block_iter);
  if(first_block.first > sa_range.first) { return; }

  // Check whether range.second ends a full block.
  pair_type next_block = this->getNextBlock(sa_range, block_iter);
  if(next_block.first != sa_range.second + 1) { return; }

  usint current = first_block.second;
  while(current < next_block.second)
  {
    pair_type ancestor = this->getAncestor(current, next_block.second);
    result.push_back(ancestor.first);
    current = ancestor.second;
  }
}

pair_type
PDLTree::getFirstBlock(pair_type sa_range) const
{
  CSA::DeltaVector::Iterator iter(*(this->leaf_ranges));
  return this->getFirstBlock(sa_range, iter);
}

pair_type
PDLTree::getFirstBlock(pair_type sa_range, CSA::DeltaVector::Iterator& iter) const
{
  return iter.valueAfter(sa_range.first);
}

pair_type
PDLTree::getLastBlock(pair_type sa_range) const
{
  CSA::DeltaVector::Iterator iter(*(this->leaf_ranges));
  pair_type last_block = iter.valueBefore(sa_range.second);
  pair_type next_block = iter.nextValue();

  // Easy case: The range ends with a full block.
  if(next_block.first == sa_range.second + 1) { return pair_type(sa_range.second, last_block.second); }

  // Annoying case: No block ends before the end of the range.
  if(last_block.second == 0) { return pair_type(0, this->getNumberOfNodes()); }

  // Degenerate case: There is some slack at the end of the range.
  return pair_type(last_block.first - 1, last_block.second - 1);
}

pair_type
PDLTree::getNextBlock(pair_type sa_range) const
{
  CSA::DeltaVector::Iterator iter(*(this->leaf_ranges));
  return this->getNextBlock(sa_range, iter);
}

pair_type
PDLTree::getNextBlock(pair_type sa_range, CSA::DeltaVector::Iterator& iter) const
{
  iter.valueBefore(sa_range.second);
  return iter.nextValue();
}

pair_type
PDLTree::getAncestor(usint block, usint next_leaf) const
{
  CSA::SuccinctVector::Iterator iter(*(this->first_children));

  pair_type current(block, block + 1);
  while(current.second < next_leaf)
  {
    if(!(iter.isSet(current.first))) { break; } // Not a first child.
    pair_type candidate = this->parentOf(current.first, iter);
    if(candidate.second > next_leaf) { break; } // The candidate is too high.
    current = candidate;
  }

  return current;
}

pair_type
PDLTree::parentOf(usint tree_node, CSA::SuccinctVector::Iterator& iter) const
{
  usint par = this->parents->readItemConst(iter.rank(tree_node) - 1);
  usint next_leaf = this->next_leaves->readItemConst(par);
  return pair_type(par + this->getNumberOfLeaves(), next_leaf);
}

//--------------------------------------------------------------------------

CSA::ReadBuffer*
readGrammar(const PDLTree& tree, const std::string& base_name, uint& terminals)
{
  if(!(tree.isOk()))
  {
    std::cerr << "readGrammar(): Invalid PDLTree!" << std::endl;
    return 0;
  }

  std::string input_name = base_name + PDLTree::SETS_EXTENSION + ".R";
  std::ifstream input(input_name.c_str(), std::ios_base::binary);
  if(!input)
  {
    std::cerr << "readGrammar(): Cannot open grammar file " << input_name << std::endl;
    return 0;
  }

  usint n = CSA::fileSize(input) / sizeof(uint) - 1;
  input.read((char*)&terminals, sizeof(terminals));
  uint* buffer = new uint[n];
  input.read((char*)buffer, n * sizeof(uint));
  input.close();

  // Convert symbols to values (remove endmarkers).
  uint endmarkers = terminals - tree.getNumberOfDocuments() - 1;
  #pragma omp parallel for schedule(static)
  for(usint i = 0; i < n; i++)
  {
    if(buffer[i] >= terminals) { buffer[i] -= endmarkers; }
//    if(tree.isNonTerminal(buffer[i])) { buffer[i] = tree.toValue(buffer[i]); }
  }

  // Compress the grammar.
  usint largest = *(std::max_element(buffer, buffer + n));
  CSA::WriteBuffer grammar(n, CSA::length(largest));
  for(usint i = 0; i < n; i++) { grammar.writeItem(buffer[i]); }
  delete[] buffer; buffer = 0;

  return grammar.getReadBuffer();
}

CSA::MultiArray*
readBlocks(const PDLTree& tree, const std::string& base_name, uint terminals)
{
  if(!(tree.isOk()))
  {
    std::cerr << "readBlocks(): Invalid PDLTree!" << std::endl;
    return 0;
  }

  std::string input_name = base_name + PDLTree::SETS_EXTENSION + ".C";
  std::ifstream input(input_name.c_str(), std::ios_base::binary);
  if(!input)
  {
    std::cerr << "readBlocks(): Cannot open block file " << input_name << std::endl;
    return 0;
  }

  usint n = CSA::fileSize(input) / sizeof(uint);
  uint* buffer = new uint[n];
  input.read((char*)buffer, n * sizeof(uint));
  input.close();

  // Determine the number of items and the largest actual item value.
  uint endmarkers = terminals - tree.getNumberOfDocuments() - 1;
  usint values = n - endmarkers;
  usint largest = *(std::max_element(buffer, buffer + n));
  if(largest >= terminals) { largest -= endmarkers; }
  else { largest = tree.getNumberOfDocuments(); }

  // Compress the blocks.
  CSA::MultiArray* blocks = CSA::MultiArray::createFixed(values, CSA::length(largest));
  bool in_set = false;
  usint curr_block = 0;
  for(usint i = 0; i < n; i++)
  {
    if(buffer[i] > tree.getNumberOfDocuments() && buffer[i] < terminals)  // endmarker
    {
      in_set = false; curr_block++;
    }
    else
    {
      if(!in_set) { blocks->nextArray(); in_set = true; }
      if(buffer[i] >= terminals) { buffer[i] -= endmarkers; }
      blocks->writeItem(buffer[i]);
    }
  }
  delete[] buffer; buffer = 0;

  blocks->finishWriting();
  return blocks;
}

//--------------------------------------------------------------------------
