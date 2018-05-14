#include <iostream>
#include <stack>

#include <sdsl/bit_vectors.hpp>

#include "drl/doclist.h"

//--------------------------------------------------------------------------

const std::string Doclist::DOCARRAY_EXTENSION = ".docs";

Doclist::Doclist(const RLCSA& _rlcsa) :
  rlcsa(_rlcsa), docarray(0), rmq(0), status(_rlcsa.isOk() ? st_unfinished : st_error)
{
}

Doclist::~Doclist()
{
  delete this->docarray; this->docarray = 0;
  delete this->rmq; this->rmq = 0;
}

Doclist::result_type*
Doclist::listDocuments(const std::string& pattern, found_type* found) const
{
  if(!(this->isOk())) { return 0; }
  pair_type range = this->rlcsa.count(pattern);
  if(CSA::isEmpty(range)) { return 0; }
  return this->listUnsafe(range, found);
}

Doclist::result_type*
Doclist::listDocuments(pair_type range, found_type* found) const
{
  if(!(this->isOk()) || CSA::isEmpty(range) || range.second >= this->rlcsa.getSize()) { return 0; }
  return this->listUnsafe(range, found);
}

Doclist::result_type*
Doclist::listUnsafe(pair_type range, found_type* found) const
{
  bool delete_found = (found == 0);
  if(delete_found) { found = new found_type(this->rlcsa.getNumberOfSequences(), 1); }

  std::stack<pair_type> intervals; intervals.push(this->getRMQRange(range));
  result_type* results = new result_type;
  while(!(intervals.empty()))
  {
    pair_type current = intervals.top(); intervals.pop();
    usint min_pos = (*(this->rmq))(current.first, current.second);
    usint doc = this->docAt(min_pos, range);
    if(!(found->isSet(doc)))
    {
      this->addDocs(doc, min_pos, results, found, range);
      if(min_pos < current.second) { intervals.push(pair_type(min_pos + 1, current.second)); }
      if(min_pos > current.first)  { intervals.push(pair_type(current.first, min_pos - 1)); }
    }
  }

  CSA::sequentialSort(results->begin(), results->end());
  if(delete_found) { delete found; found = 0; }
  else
  {
    for(result_type::iterator iter = results->begin(); iter != results->end(); ++iter)
    {
      found->unsetBit(*iter);
    }
  }
  return results;
}

Doclist::result_type*
Doclist::listDocumentsBrute(const std::string& pattern) const
{
  if(!(this->isOk())) { return 0; }
  pair_type range = this->rlcsa.count(pattern);
  if(CSA::isEmpty(range)) { return 0; }
  return this->listUnsafeBrute(range);
}

Doclist::result_type*
Doclist::listDocumentsBrute(pair_type range) const
{
  if(!(this->isOk()) || CSA::isEmpty(range) || range.second >= this->rlcsa.getSize()) { return 0; }
  return this->listUnsafeBrute(range);
}

Doclist::result_type*
Doclist::listUnsafeBrute(pair_type range) const
{
  result_type* results = new result_type;
  if(this->hasDocArray())
  {
    results->reserve(CSA::length(range));
    for(usint i = range.first; i <= range.second; i++)
    {
      results->push_back(this->docarray->readItemConst(i));
    }
  }
  else
  {
    results->resize(CSA::length(range));
    usint* buffer = results->data();
    buffer = this->rlcsa.getSequenceForPosition(this->rlcsa.locate(range, buffer), CSA::length(range));
  }

  CSA::removeDuplicates(results, false);
  return results;
}

usint
Doclist::commonStructureSize() const
{
  usint bytes = 0;
  if(this->rmq != 0) { bytes += size_in_bytes(*(this->rmq)); }
  if(this->docarray != 0) { bytes += this->docarray->reportSize(); }
  return bytes;
}

usint
Doclist::commonStructureSizeBrute() const
{
  usint bytes = 0;
  if(this->docarray != 0) { bytes += this->docarray->reportSize(); }
  return bytes;
}

void
Doclist::readRMQ(std::ifstream& input)
{
  delete this->rmq; this->rmq = 0;
  this->rmq = new RMQType;
  this->rmq->load(input);
}

void
Doclist::writeRMQ(std::ofstream& output) const
{
  if(!(this->isOk())) { return; }
  this->rmq->serialize(output);
}

void
Doclist::readDocs(const std::string& base_name)
{
  if(!(this->isOk())) { return; }

  delete this->docarray; this->docarray = 0;
  this->docarray = readDocArray(this->rlcsa, base_name);
}

void
Doclist::readDocs(std::ifstream& input)
{
  if(!(this->isOk())) { return; }

  delete this->docarray; this->docarray = 0;
  this->docarray = readDocArray(this->rlcsa, input);
}

void
Doclist::writeDocs(const std::string& base_name) const
{
  if(!(this->isOk()) || this->docarray == 0) { return; }

  std::string docarray_name = base_name + DOCARRAY_EXTENSION;
  std::ofstream output(docarray_name.c_str(), std::ios_base::binary);
  if(output)
  {
    this->writeDocs(output);
    output.close();
  }
}

void
Doclist::writeDocs(std::ofstream& output) const
{
  if(!(this->isOk()) || this->docarray == 0) { return; }
  this->docarray->writeBuffer(output);
}

//--------------------------------------------------------------------------

const std::string DoclistSada::EXTENSION = ".sada";

DoclistSada::DoclistSada(const RLCSA& _rlcsa, bool store_docarray) :
  Doclist(_rlcsa)
{
  if(this->status != st_unfinished || !(this->rlcsa.supportsLocate())) { return; }

  // Build the C array and store the D array.
  usint* prev = new usint[this->rlcsa.getNumberOfSequences()];
  for(usint i = 0; i < this->rlcsa.getNumberOfSequences(); i++) { prev[i] = 0; }
  sdsl::int_vector<64> c_array(this->rlcsa.getSize());
  CSA::WriteBuffer* buffer = 0;
  if(store_docarray)
  {
    usint item_bits = std::max((usint)1, CSA::length(this->rlcsa.getNumberOfSequences() - 1));
    buffer = new CSA::WriteBuffer(this->rlcsa.getSize(), item_bits);
  }

  for(usint i = 0; i < this->rlcsa.getSize(); i += BLOCK_SIZE)
  {
    pair_type range(i, std::min(i + BLOCK_SIZE, this->rlcsa.getSize()) - 1);
    usint* docs = this->rlcsa.locate(range);
    this->rlcsa.getSequenceForPosition(docs, CSA::length(range));
    for(usint j = range.first; j <= range.second; j++)
    {
      c_array[j] = prev[docs[j - i]];
      prev[docs[j - i]] = j + 1;
      if(store_docarray) { buffer->writeItem(docs[j - i]); }
    }
    delete[] docs; docs = 0;
  }
  if(store_docarray)
  {
    this->docarray = buffer->getReadBuffer();
    delete buffer; buffer = 0;
  }
  delete[] prev; prev = 0;

  // Build the RMQ.
  this->rmq = new RMQType(&c_array);

  this->status = st_ok;
}

DoclistSada::DoclistSada(const RLCSA& _rlcsa, const std::string& base_name, bool load_docarray) :
  Doclist(_rlcsa)
{
  if(this->status != st_unfinished || !(this->rlcsa.supportsLocate())) { return; }

  std::string doclist_name = base_name + EXTENSION;
  std::ifstream input(doclist_name.c_str(), std::ios_base::binary);
  if(input)
  {
    this->loadFrom(input);
    if(load_docarray) { this->readDocs(base_name); }
    input.close();
  }
}

DoclistSada::DoclistSada(const RLCSA& _rlcsa, std::ifstream& input, bool load_docarray) :
  Doclist(_rlcsa)
{
  if(this->status != st_unfinished || !(this->rlcsa.supportsLocate())) { return; }
  this->loadFrom(input);
  if(load_docarray) { this->readDocs(input); }
}

DoclistSada::~DoclistSada()
{
}

void
DoclistSada::writeTo(const std::string& base_name) const
{
  if(!(this->isOk())) { return; }
  std::string doclist_name = base_name + EXTENSION;
  std::ofstream output(doclist_name.c_str(), std::ios_base::binary);
  if(output)
  {
    this->writeRMQ(output);
    this->writeDocs(base_name);
    output.close();
  }
}

void
DoclistSada::writeTo(std::ofstream& output) const
{
  if(!(this->isOk())) { return; }
  this->writeRMQ(output);
  this->writeDocs(output);
}

void
DoclistSada::loadFrom(std::ifstream& input)
{
  this->readRMQ(input);
  this->status = st_ok;
}

usint
DoclistSada::reportSize() const
{
  return sizeof(*this) + this->commonStructureSize();
}

usint
DoclistSada::reportSizeBrute() const
{
  return sizeof(*this) + this->commonStructureSizeBrute();
}

pair_type
DoclistSada::getRMQRange(pair_type sa_range) const
{
  return sa_range;
}

usint
DoclistSada::docAt(usint rmq_index, pair_type) const
{
  if(this->hasDocArray()) { return this->docarray->readItemConst(rmq_index); }
  return this->rlcsa.getSequenceForPosition(this->rlcsa.locate(rmq_index));
}

void
DoclistSada::addDocs(usint first_doc, usint, result_type* results, found_type* found, pair_type) const
{
  results->push_back(first_doc);
  found->setBit(first_doc);
}

//--------------------------------------------------------------------------

const std::string DoclistILCP::EXTENSION = ".ilcp";

DoclistILCP::DoclistILCP(const RLCSA& _rlcsa) :
  Doclist(_rlcsa), run_heads(0)
{
  if(this->status != st_unfinished || !(this->rlcsa.supportsLocate())) { return; }

  // Build ILCP
  uint* iLCP = new uint[this->rlcsa.getSize()];
  for(usint i = 0; i < this->rlcsa.getNumberOfSequences(); i++)
  {
    CSA::SuffixArray* sa = this->rlcsa.getSuffixArrayForSequence(i);
    uint* plcp = sa->getLCPArray(true);
    for(usint j = 0; j < sa->getSize(); j++)
    {
      iLCP[sa->getRanks()[j] - this->rlcsa.getNumberOfSequences()] = plcp[j];
    }
    delete[] plcp; plcp = 0;
    delete sa; sa = 0;
  }

  // Count run heads
  usint n_run_heads = 1;
  for(usint i = 1; i < this->rlcsa.getSize(); i++)
  {
    if(iLCP[i] != iLCP[i - 1]) { n_run_heads++; }
  }

  // Build RMQ for the run heads
  sdsl::int_vector<32> heads(n_run_heads); heads[0] = iLCP[0];
  CSA::DeltaVector::Encoder encoder(DV_BLOCK_SIZE); encoder.setBit(0);
  for(usint i = 1, j = 1; i < this->rlcsa.getSize(); i++)
  {
    if(iLCP[i] != iLCP[i - 1])
    {
      heads[j] = iLCP[i]; j++;
      encoder.setBit(i);
    }
  }
  this->rmq = new RMQType(&heads);
  this->run_heads = new CSA::DeltaVector(encoder, this->rlcsa.getSize());

  this->status = st_ok;
}

DoclistILCP::DoclistILCP(const RLCSA& _rlcsa, const std::string& base_name, bool load_docarray) :
  Doclist(_rlcsa), run_heads(0)
{
  if(this->status != st_unfinished || !(this->rlcsa.supportsLocate())) { return; }

  std::string doclist_name = base_name + EXTENSION;
  std::ifstream input(doclist_name.c_str(), std::ios_base::binary);
  if(input)
  {
    this->loadFrom(input);
    if(load_docarray) { this->readDocs(base_name); }
    input.close();
  }
}

DoclistILCP::DoclistILCP(const RLCSA& _rlcsa, std::ifstream& input, bool load_docarray) :
  Doclist(_rlcsa), run_heads(0)
{
  if(this->status != st_unfinished || !(this->rlcsa.supportsLocate())) { return; }
  this->loadFrom(input);
  if(load_docarray) { this->readDocs(input); }
}

DoclistILCP::~DoclistILCP()
{
  delete this->run_heads; this->run_heads = 0;
}

void
DoclistILCP::writeTo(const std::string& base_name) const
{
  if(!(this->isOk())) { return; }
  std::string doclist_name = base_name + EXTENSION;
  std::ofstream output(doclist_name.c_str(), std::ios_base::binary);
  if(output)
  {
    this->writeRMQ(output);
    this->run_heads->writeTo(output);
    this->writeDocs(base_name);
    output.close();
  }
}

void
DoclistILCP::writeTo(std::ofstream& output) const
{
  if(!(this->isOk())) { return; }
  this->writeRMQ(output);
  this->run_heads->writeTo(output);
  this->writeDocs(output);
}

void
DoclistILCP::loadFrom(std::ifstream& input)
{
  this->readRMQ(input);
  this->run_heads = new CSA::DeltaVector(input);
  this->status = st_ok;
}

usint
DoclistILCP::reportSize() const
{
  usint run_head_bytes = (this->run_heads == 0 ? 0 : this->run_heads->reportSize());
  return sizeof(*this) + this->commonStructureSize() + run_head_bytes;
}

usint
DoclistILCP::reportSizeBrute() const
{
  return sizeof(*this) + this->commonStructureSizeBrute();
}

pair_type
DoclistILCP::getRMQRange(pair_type sa_range) const
{
  CSA::DeltaVector::Iterator iter(*(this->run_heads));
  return pair_type(iter.rank(sa_range.first) - 1, iter.rank(sa_range.second) - 1);
}

usint
DoclistILCP::docAt(usint rmq_index, pair_type sa_range) const
{
  CSA::DeltaVector::Iterator iter(*(this->run_heads));
  usint sa_index = std::max(sa_range.first, iter.select(rmq_index));
  if(this->hasDocArray()) { return this->docarray->readItemConst(sa_index); }
  return this->rlcsa.getSequenceForPosition(this->rlcsa.locate(sa_index));
}

void
DoclistILCP::addDocs(usint first_doc, usint rmq_index, result_type* results, found_type* found, pair_type sa_range) const
{
  results->push_back(first_doc); found->setBit(first_doc);

  CSA::DeltaVector::Iterator iter(*(this->run_heads));
  pair_type range(std::max(sa_range.first, iter.select(rmq_index)) + 1, 0);
  range.second = std::min(sa_range.second, iter.selectNext() - 1);
  if(CSA::isEmpty(range)) { return; }

  if(this->hasDocArray())
  {
    for(usint i = range.first; i <= range.second; i++)
    {
      usint doc = this->docarray->readItemConst(i);
      results->push_back(doc);
      found->setBit(doc);
    }
  }
  else
  {
    usint* res = this->rlcsa.locate(range);
    this->rlcsa.getSequenceForPosition(res, CSA::length(range));
    for(usint i = range.first; i <= range.second; i++)
    {
      results->push_back(res[i - range.first]);
      found->setBit(res[i - range.first]);
    }
    delete[] res; res = 0;
  }
}

//--------------------------------------------------------------------------
