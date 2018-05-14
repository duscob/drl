/*
  Copyright (c) 2013, 2014 Jouni Sirén
  Copyright (c) 2016 Genome Research Ltd.

  Author: Jouni Sirén <jouni.siren@iki.fi>

  Permission is hereby granted, free of charge, to any person obtaining a copy
  of this software and associated documentation files (the "Software"), to deal
  in the Software without restriction, including without limitation the rights
  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  copies of the Software, and to permit persons to whom the Software is
  furnished to do so, subject to the following conditions:

  The above copyright notice and this permission notice shall be included in all
  copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
  SOFTWARE.
*/

#include <iostream>
#include <sstream>

#include "drl/utils.h"

//--------------------------------------------------------------------------

CSA::ReadBuffer*
readDocArray(const RLCSA& rlcsa, const std::string& base_name)
{
  const static std::string DOCARRAY_EXTENSION = ".docs";

  std::string docarray_name = base_name + DOCARRAY_EXTENSION;
  std::ifstream input(docarray_name.c_str(), std::ios_base::binary);
  if(!input)
  {
    std::cerr << "readDocArray(): Cannot open input file " << docarray_name << std::endl;
    return 0;
  }

  CSA::ReadBuffer* docarray = readDocArray(rlcsa, input);
  input.close();
  return docarray;
}

CSA::ReadBuffer*
readDocArray(const RLCSA& rlcsa, std::ifstream& input)
{
  usint item_bits = std::max((usint)1, CSA::length(rlcsa.getNumberOfSequences() - 1));
  return new CSA::ReadBuffer(input, rlcsa.getSize(), item_bits);
}

//--------------------------------------------------------------------------

void
printHeader(const std::string& header, usint indent)
{
  std::string padding;
  if(header.length() + 1 < indent) { padding = std::string(indent - 1 - header.length(), ' '); }
  std::cout << header << ":" << padding;
}

void
printResults(const std::string& algorithm, double seconds, usint docc, usint patterns, usint indent)
{
  printHeader(algorithm, indent);
  std::cout << docc << " documents in " << seconds << " seconds ("
            << (docc / seconds) << " docc/s, "
            << (patterns / seconds) << " patterns/s)" << std::endl;
}

void
printSize(const std::string& algorithm, usint text_size, usint index_size, usint doclist_size, usint indent)
{
  printHeader(algorithm, indent);

  std::cout << CSA::inMegabytes(doclist_size) << " MB / "
            << CSA::inBPC(doclist_size, text_size) << " bpc (total "
            << CSA::inMegabytes(doclist_size + index_size) << " MB / "
            << CSA::inBPC(doclist_size + index_size, text_size) << " bpc)" << std::endl;
}

void
printSizeNoCSA(const std::string& algorithm, usint text_size, usint doclist_size, usint indent)
{
  printHeader(algorithm, indent);

  std::cout << CSA::inMegabytes(doclist_size) << " MB / "
            << CSA::inBPC(doclist_size, text_size) << " bpc" << std::endl;
}

void
printCount(const std::string& algorithm, usint text_size, usint doclist_size, double seconds, usint patterns)
{
  std::cout << algorithm << ";"
            << CSA::inBPC(doclist_size, text_size) << ";"
            << CSA::inNanoseconds(seconds / patterns) << std::endl;
}

//--------------------------------------------------------------------------

void
sortById(std::vector<Document>* docs)
{
  CSA::sequentialSort(docs->begin(), docs->end());
}

struct MostFrequentComparator
{
  bool operator() (const Document& a, const Document& b) const
  {
    // Descending by frequency, ascending by id.
    if(a.second != b.second) { return (a.second > b.second); }
    return (a.first < b.first);
  }
} mf_comparator;

void
sortByFrequency(std::vector<Document>* docs)
{
  CSA::sequentialSort(docs->begin(), docs->end(), mf_comparator);
}

void
mergeDocumentRuns(std::vector<Document>* docs)
{
  if(docs->size() < 2) { return; }

  std::vector<Document>::iterator curr = docs->begin() + 1, tail = docs->begin();
  while(curr != docs->end())
  {
    if(curr->first == tail->first) { tail->second += curr->second; }
    else { ++tail; *tail = *curr; }
    ++curr;
  }

  docs->resize((tail - docs->begin()) + 1);
}

//--------------------------------------------------------------------------

struct ScoredDocIdComparator
{
  inline bool operator() (const ScoredDoc& a, const ScoredDoc& b) const
  {
    return (a.id < b.id);
  }
} sdi_comparator;

void
sortById(std::vector<ScoredDoc>& docs)
{
  CSA::sequentialSort(docs.begin(), docs.end(), sdi_comparator);
}

struct ScoredDocScoreComparator
{
  inline bool operator() (const ScoredDoc& a, const ScoredDoc& b) const
  {
    if(a.score != b.score) { return (a.score > b.score); }
    else { return (a.id < b.id); }
  }
} sds_comparator;

void
sortByScore(std::vector<ScoredDoc>& docs)
{
  CSA::sequentialSort(docs.begin(), docs.end(), sds_comparator);
}

void
mergeDocumentRuns(std::vector<ScoredDoc>& docs)
{
  if(docs.size() < 2) { return; }

  usint tail = 0;
  for(usint i = 1; i < docs.size(); i++)
  {
    if(docs[i].id == docs[tail].id) { docs[tail].terms += docs[i].terms; docs[tail].score += docs[i].score; }
    else { tail++; docs[tail] = docs[i]; }
  }
  docs.resize(tail + 1);
}

void
filterByMask(std::vector<ScoredDoc>& docs, ScoredDoc::mask_type mask)
{
  usint tail = 0;
  for(usint i = 0; i < docs.size(); i++)
  {
    if((docs[i].terms & mask) == mask) { docs[tail] = docs[i]; tail++; }
  }
  docs.resize(tail);
}

//--------------------------------------------------------------------------

std::vector<uint>*
bruteForceDocListUnsafe(const RLCSA* rlcsa, const CSA::ReadBuffer* docarray, pair_type sa_range, std::vector<uint>* result)
{
  bool filter = (result == 0);
  if(result == 0) { result = new std::vector<uint>; result->reserve(CSA::length(sa_range)); }

  if(docarray == 0)
  {
    for(usint offset = sa_range.first; offset <= sa_range.second; offset += CSA::MILLION)
    {
      usint len = std::min(CSA::MILLION, sa_range.second + 1 - offset);
      usint* buffer = rlcsa->getSequenceForPosition(rlcsa->locate(sa_range), len);
      for(usint i = 0; i < len; i++) { result->push_back(buffer[i]); }
      delete[] buffer; buffer = 0;
    }
  }
  else
  {
    for(usint i = sa_range.first; i <= sa_range.second; i++)
    {
      result->push_back(docarray->readItemConst(i));
    }
  }

  if(filter) { CSA::removeDuplicates(result, false); }

  return result;
}

std::vector<uint>*
bruteForceDocList(const RLCSA& rlcsa, const std::string& pattern, std::vector<uint>* result)
{
  if(!(rlcsa.isOk())) { return result; }
  pair_type sa_range = rlcsa.count(pattern);
  if(CSA::isEmpty(sa_range)) { return result; }
  return bruteForceDocListUnsafe(&rlcsa, 0, sa_range, result);
}

std::vector<uint>*
bruteForceDocList(const RLCSA& rlcsa, pair_type sa_range, std::vector<uint>* result)
{
  if(!(rlcsa.isOk()) || CSA::isEmpty(sa_range) || sa_range.second >= rlcsa.getSize()) { return result; }
  return bruteForceDocListUnsafe(&rlcsa, 0, sa_range, result);
}

std::vector<uint>*
bruteForceDocList(const RLCSA& rlcsa, const CSA::ReadBuffer& docarray, const std::string& pattern, std::vector<uint>* result)
{
  if(!(rlcsa.isOk())) { return result; }
  pair_type sa_range = rlcsa.count(pattern);
  if(CSA::isEmpty(sa_range)) { return result; }
  return bruteForceDocListUnsafe(0, &docarray, sa_range, result);
}

std::vector<uint>*
bruteForceDocList(const CSA::ReadBuffer& docarray, pair_type sa_range, std::vector<uint>* result)
{
  if(CSA::isEmpty(sa_range) || sa_range.second >= docarray.getNumberOfItems()) { return result; }
  return bruteForceDocListUnsafe(0, &docarray, sa_range, result);
}

//--------------------------------------------------------------------------

usint
bruteForceDocCountUnsafe(const RLCSA* rlcsa, const CSA::ReadBuffer* docarray, pair_type sa_range)
{
  std::vector<uint> result;

  if(docarray == 0)
  {
    for(usint offset = sa_range.first; offset <= sa_range.second; offset += CSA::MILLION)
    {
      usint len = std::min(CSA::MILLION, sa_range.second + 1 - offset);
      usint* buffer = rlcsa->getSequenceForPosition(rlcsa->locate(sa_range), len);
      for(usint i = 0; i < len; i++) { result.push_back(buffer[i]); }
      delete[] buffer; buffer = 0;
    }
  }
  else
  {
    for(usint i = sa_range.first; i <= sa_range.second; i++)
    {
      result.push_back(docarray->readItemConst(i));
    }
  }

  CSA::removeDuplicates(result, false);
  return result.size();
}

usint
bruteForceDocCount(const RLCSA& rlcsa, const std::string& pattern)
{
  if(!(rlcsa.isOk())) { return 0; }
  pair_type sa_range = rlcsa.count(pattern);
  if(CSA::isEmpty(sa_range)) { return 0; }
  return bruteForceDocCountUnsafe(&rlcsa, 0, sa_range);
}

usint
bruteForceDocCount(const RLCSA& rlcsa, pair_type sa_range)
{
  if(!(rlcsa.isOk()) || CSA::isEmpty(sa_range) || sa_range.second >= rlcsa.getSize()) { return 0; }
  return bruteForceDocCountUnsafe(&rlcsa, 0, sa_range);
}

usint bruteForceDocCount(const RLCSA& rlcsa, const CSA::ReadBuffer& docarray, const std::string& pattern)
{
  if(!(rlcsa.isOk())) { return 0; }
  pair_type sa_range = rlcsa.count(pattern);
  if(CSA::isEmpty(sa_range)) { return 0; }
  return bruteForceDocCountUnsafe(0, &docarray, sa_range);
}

usint
bruteForceDocCount(const CSA::ReadBuffer& docarray, pair_type sa_range)
{
  if(CSA::isEmpty(sa_range) || sa_range.second >= docarray.getNumberOfItems()) { return 0; }
  return bruteForceDocCountUnsafe(0, &docarray, sa_range);
}

//--------------------------------------------------------------------------

std::vector<Document>*
bruteForceTopkUnsafe(const RLCSA* rlcsa, const CSA::ReadBuffer* docarray, pair_type sa_range, usint k)
{
  std::vector<Document>* results = new std::vector<Document>;
  results->reserve(CSA::length(sa_range));

  if(docarray == 0)
  {
    for(usint offset = sa_range.first; offset <= sa_range.second; offset += CSA::MILLION)
    {
      usint len = std::min(CSA::MILLION, sa_range.second + 1 - offset);
      usint* buffer = rlcsa->getSequenceForPosition(rlcsa->locate(sa_range), len);
      for(usint i = 0; i < len; i++) { results->push_back(Document(buffer[i], 1)); }
      delete[] buffer; buffer = 0;
    }
  }
  else
  {
    for(usint i = sa_range.first; i <= sa_range.second; i++)
    {
      results->push_back(Document(docarray->readItemConst(i), 1));
    }
  }

  sortById(results);
  mergeDocumentRuns(results);
  sortByFrequency(results);
  if(results->size() > k) { results->resize(k); }

  return results;
}

std::vector<Document>*
bruteForceTopk(const RLCSA& rlcsa, const std::string& pattern, usint k)
{
  if(!(rlcsa.isOk())) { return 0; }
  pair_type sa_range = rlcsa.count(pattern);
  if(CSA::isEmpty(sa_range)) { return 0; }
  return bruteForceTopkUnsafe(&rlcsa, 0, sa_range, k);
}

std::vector<Document>*
bruteForceTopk(const RLCSA& rlcsa, pair_type sa_range, usint k)
{
  if(!(rlcsa.isOk()) || CSA::isEmpty(sa_range) || sa_range.second >= rlcsa.getSize()) { return 0; }
  return bruteForceTopkUnsafe(&rlcsa, 0, sa_range, k);
}

std::vector<Document>*
bruteForceTopk(const RLCSA& rlcsa, const CSA::ReadBuffer& docarray, const std::string& pattern, usint k)
{
  if(!(rlcsa.isOk())) { return 0; }
  pair_type sa_range = rlcsa.count(pattern);
  if(CSA::isEmpty(sa_range)) { return 0; }
  return bruteForceTopkUnsafe(0, &docarray, sa_range, k);
}

std::vector<Document>*
bruteForceTopk(const CSA::ReadBuffer& docarray, pair_type sa_range, usint k)
{
  if(CSA::isEmpty(sa_range) || sa_range.second >= docarray.getNumberOfItems()) { return 0; }
  return bruteForceTopkUnsafe(0, &docarray, sa_range, k);
}

//--------------------------------------------------------------------------

void
parseTREC(const std::string& query, std::vector<std::string>& terms)
{
  terms.clear();

  std::string term;
  std::istringstream ss(query);
  std::getline(ss, term, ':');  // Skip the query identifier.
  while(std::getline(ss, term, ' ')) { terms.push_back(term); }
}

//--------------------------------------------------------------------------
