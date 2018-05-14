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

#ifndef _DOCLIST_UTILS_H
#define _DOCLIST_UTILS_H

#include <fstream>
#include <vector>

#include <rlcsa/rlcsa.h>

//--------------------------------------------------------------------------

using CSA::usint;
using CSA::pair_type;
using CSA::RLCSA;

#ifndef uint
typedef CSA::uint uint;
#endif

// This is also defined in namespace CSA.
template<class A, class B>
std::ostream& operator<<(std::ostream& stream, const std::pair<A, B>& data)
{
  return stream << "(" << data.first << ", " << data.second << ")";
}

//--------------------------------------------------------------------------

const usint DEFAULT_INDENT = 17;

void printHeader(const std::string& header, usint indent = DEFAULT_INDENT);
void printResults(const std::string& algorithm, double seconds, usint docc, usint patterns, usint indent = DEFAULT_INDENT);
void printSize(const std::string& algorithm, usint text_size, usint index_size, usint doclist_size, usint indent = DEFAULT_INDENT);
void printSizeNoCSA(const std::string& algorithm, usint text_size, usint doclist_size, usint indent = DEFAULT_INDENT);
void printCount(const std::string& algorithm, usint text_size, usint doclist_size, double seconds, usint patterns);

//--------------------------------------------------------------------------

typedef std::pair<uint, uint> Document; // (id, freq)

// User must delete the returned buffer.
CSA::ReadBuffer* readDocArray(const RLCSA& rlcsa, const std::string& base_name);
CSA::ReadBuffer* readDocArray(const RLCSA& rlcsa, std::ifstream& input);

void sortById(std::vector<Document>* docs);

// Descending by freq, then ascending by id.
// Sorting by (freq, id) is used for PDL-topk construction.
void sortByFrequency(std::vector<Document>* docs);

// Merge runs of identical documents.
void mergeDocumentRuns(std::vector<Document>* docs);

// User must delete the returned vector.
// If a result vector is given, the return value is always the same vector. In that case, the user
// is responsible for filtering out duplicates.
std::vector<uint>* bruteForceDocList(const RLCSA& rlcsa, const std::string& pattern, std::vector<uint>* result = 0);
std::vector<uint>* bruteForceDocList(const RLCSA& rlcsa, pair_type sa_range, std::vector<uint>* result = 0);
std::vector<uint>* bruteForceDocList(const RLCSA& rlcsa, const CSA::ReadBuffer& docarray, const std::string& pattern, std::vector<uint>* result = 0);
std::vector<uint>* bruteForceDocList(const CSA::ReadBuffer& docarray, pair_type sa_range, std::vector<uint>* result = 0);

usint bruteForceDocCount(const RLCSA& rlcsa, const std::string& pattern);
usint bruteForceDocCount(const RLCSA& rlcsa, pair_type sa_range);
usint bruteForceDocCount(const RLCSA& rlcsa, const CSA::ReadBuffer& docarray, const std::string& pattern);
usint bruteForceDocCount(const CSA::ReadBuffer& docarray, pair_type sa_range);

// User must delete the returned vector.
std::vector<Document>* bruteForceTopk(const RLCSA& rlcsa, const std::string& pattern, usint k);
std::vector<Document>* bruteForceTopk(const RLCSA& rlcsa, pair_type sa_range, usint k);
std::vector<Document>* bruteForceTopk(const RLCSA& rlcsa, const CSA::ReadBuffer& docarray, const std::string& pattern, usint k);
std::vector<Document>* bruteForceTopk(const CSA::ReadBuffer& docarray, pair_type sa_range, usint k);

//--------------------------------------------------------------------------

struct ScoredDoc
{
  typedef usint mask_type;

  const static usint MAX_TERMS = CHAR_BIT * sizeof(mask_type);

  usint     id;
  mask_type terms;
  double    score;

  ScoredDoc() {}

  // For topk-merge. This variant uses 'terms' as the number of blocks.
  ScoredDoc(Document doc) :
    id(doc.first), terms(1), score(doc.second)
  {
  }

  // For tf-idf. This variant uses 'terms' as a bit mask.
  ScoredDoc(Document doc, usint term, double idf) :
    id(doc.first), terms(((mask_type)1) << term), score(doc.second * idf)
  {
  }

  inline Document getDoc() { return Document(id, score); }
};

void sortById(std::vector<ScoredDoc>& docs);

// Descending by score, then ascending by id.
void sortByScore(std::vector<ScoredDoc>& docs);

// Merge runs of identical documents.
void mergeDocumentRuns(std::vector<ScoredDoc>& docs);

// Remove documents not matching the terms included in the mask.
void filterByMask(std::vector<ScoredDoc>& docs, ScoredDoc::mask_type mask);

//--------------------------------------------------------------------------

// Parse a TREC 2006 terabyte track efficiency query into terms.
void parseTREC(const std::string& query, std::vector<std::string>& terms);

//--------------------------------------------------------------------------

#endif  // _DOCLIST_UTILS_H
