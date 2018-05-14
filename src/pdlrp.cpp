#include <iostream>
#include <stack>

#include "drl/pdlrp.h"

//--------------------------------------------------------------------------

const std::string PDLRP::EXTENSION = ".pdlrp";
const std::string PDLRP::SET_EXTENSION = ".pdlset";

PDLRP::PDLRP(const RLCSA& _rlcsa, const std::string& base_name, bool compress_sets, bool use_rpset) :
  rlcsa(_rlcsa), status(st_error),
  tree(0), grammar(0), blocks(0)
{
  if(!(this->rlcsa.isOk()) || !(this->rlcsa.supportsLocate()))
  {
    std::cerr << "PDLRP::PDLRP(): Invalid RLCSA!" << std::endl;
    return;
  }

  std::string input_name = base_name;
  if(compress_sets)  { input_name += PDLTree::EXTENSION; }
  else if(use_rpset) { input_name += SET_EXTENSION; }
  else               { input_name += EXTENSION; }
  std::ifstream input(input_name.c_str(), std::ios_base::binary);
  if(!input)
  {
    std::cerr << "PDLRP::PDLRP(): Cannot open input file " << input_name << std::endl;
    return;
  }
  this->tree = new PDLTree(this->rlcsa, input);
  if(!(this->tree->isOk())) { return; }
  this->status = st_unfinished;

  if(compress_sets)
  {
    uint terminals = 0;
    this->grammar = readGrammar(*(this->tree), base_name, terminals);
    this->blocks = readBlocks(*(this->tree), base_name, terminals);
  }
  else
  {
    pair_type temp;
    input.read((char*)&temp, sizeof(temp)); // Items, bits.
    this->grammar = new CSA::ReadBuffer(input, temp.first, temp.second);
    this->blocks = CSA::MultiArray::readFrom(input);
  }
  input.close();
  if(this->blocks == 0 || !(this->blocks->isOk())) { return; }

  this->status = st_ok;
}

PDLRP::~PDLRP()
{
  delete this->tree; this->tree = 0;
  delete this->grammar; this->grammar = 0;
  delete this->blocks; this->blocks = 0;
}

void
PDLRP::writeTo(const std::string& base_name, bool use_rpset) const
{
  if(!(this->isOk()))
  {
    std::cerr << "PDLRP::writeTo(): Status is not ok!" << std::endl;
    return;
  }

  std::string filename = base_name + (use_rpset ? SET_EXTENSION : EXTENSION );
  std::ofstream output(filename.c_str(), std::ios_base::binary);
  if(!output)
  {
    std::cerr << "PDLRP::writeTo(): Cannot open output file " << filename << std::endl;
    return;
  }

  this->tree->writeTo(output);

  pair_type temp(this->grammar->getNumberOfItems(), this->grammar->getItemSize());
  output.write((char*)&temp, sizeof(temp));
  this->grammar->writeTo(output);
  this->blocks->writeTo(output);

  output.close();
}

usint
PDLRP::reportSize() const
{
  usint bytes = sizeof(*this) + this->tree->reportSize() + this->grammar->reportSize() + this->blocks->reportSize();
  return bytes;
}

//--------------------------------------------------------------------------

PDLRP::result_type*
PDLRP::query(const std::string& pattern) const
{
  if(!(this->isOk())) { return 0; }
  pair_type sa_range = this->rlcsa.count(pattern);
  if(CSA::isEmpty(sa_range)) { return 0; }
  return this->queryUnsafe(sa_range);
}

PDLRP::result_type*
PDLRP::query(pair_type sa_range) const
{
  if(!(this->isOk()) || CSA::isEmpty(sa_range) || sa_range.second >= this->rlcsa.getSize()) { return 0; }
  return this->queryUnsafe(sa_range);
}

usint
PDLRP::count(const std::string& pattern) const
{
  if(!(this->isOk())) { return 0; }
  return this->count(this->rlcsa.count(pattern));
}

usint
PDLRP::count(pair_type sa_range) const
{
  if(!(this->isOk()) || CSA::isEmpty(sa_range) || sa_range.second >= this->rlcsa.getSize()) { return 0; }

  result_type* res = this->queryUnsafe(sa_range);
  if(res == 0) { return 0; }
  usint num = res->size();
  delete res; res = 0;

  return num;
}

PDLRP::result_type*
PDLRP::queryUnsafe(pair_type sa_range) const
{
  result_type* result = new result_type;

  // Process the part of the range before the first full block.
  pair_type first_block = this->tree->getFirstBlock(sa_range);
  if(first_block.first > sa_range.first)
  {
    usint temp = std::min(sa_range.second, first_block.first - 1);
    bruteForceDocList(this->rlcsa, pair_type(sa_range.first, temp), result);
    if(sa_range.second <= temp) { CSA::removeDuplicates(result, false); return result; }
  }
  usint current = first_block.second; // Current block.

  // Process the part of the range after the last full block.
  pair_type last_block = this->tree->getLastBlock(sa_range);
  if(last_block.second == this->tree->getNumberOfNodes()) // No block ends before the end of the range.
  {
    bruteForceDocList(this->rlcsa, pair_type(first_block.first, sa_range.second), result);
    CSA::removeDuplicates(result, false);
    return result;
  }
  if(last_block.first < sa_range.second)
  {
    bruteForceDocList(this->rlcsa, pair_type(last_block.first + 1, sa_range.second), result);
  }
  usint limit = last_block.second + 1;  // First block not in the range.

  // Decompress the blocks.
  usint run_start = 0, run_length = 0; // Current run of leaf blocks.
  while(current < limit)
  {
    pair_type ancestor = this->tree->getAncestor(current, limit);
    if(ancestor.first == current) // Leaf block.
    {
      if(run_length == 0) { run_start = current; run_length = 1; }
      else { run_length++; }
      current++;
    }
    else
    {
      if(run_length > 0)
      {
        if(this->addBlocks(run_start, run_length, result)) { return result; }
        run_length = 0;
      }
      if(this->addBlocks(ancestor.first, 1, result)) { return result; }
      current = ancestor.second;
    }
  }
  if(run_length > 0)
  {
    if(this->addBlocks(run_start, run_length, result)) { return result; }
    run_length = 0;
  }

  CSA::removeDuplicates(result, false);
  return result;
}

void
allDocuments(PDLRP::result_type* result, usint n)
{
  PDLRP::result_type buffer; buffer.reserve(n);
  for(usint i = 0; i < n; i++) { buffer.push_back(i); }
  result->swap(buffer);
}

bool
PDLRP::addBlocks(usint first_block, usint number_of_blocks, result_type* result) const
{
  CSA::MultiArray::Iterator* iter = this->blocks->getIterator();
  iter->goToItem(first_block, 0); iter->setEnd(first_block + number_of_blocks, 0);

  std::stack<usint> buffer;
  while(!(iter->atEnd()))
  {
    buffer.push(iter->nextItem());
    while(!(buffer.empty()))
    {
      usint value = buffer.top(); buffer.pop();
      if(this->tree->isTerminal(value))
      {
        if(value == this->tree->getNumberOfDocuments())
        {
          delete iter; iter = 0;
          allDocuments(result, this->tree->getNumberOfDocuments());
          return true;
        }
        else { result->push_back(value); }
      }
      else
      {
        value = this->tree->toRule(value);
        buffer.push(this->grammar->readItemConst(2 * value + 1));
        buffer.push(this->grammar->readItemConst(2 * value));
      }
    }
  }

  delete iter; iter = 0;
  return false;
}

//--------------------------------------------------------------------------
