#ifndef _DOCLIST_DOCLIST_H
#define _DOCLIST_DOCLIST_H

#include <sdsl/rmq_support.hpp>

#include "utils.h"

//--------------------------------------------------------------------------

class Doclist
{
  public:
    typedef std::vector<usint>                      result_type;
    typedef CSA::WriteBuffer                        found_type;
    typedef enum { st_unfinished, st_error, st_ok } status_type;

    const static std::string DOCARRAY_EXTENSION;

    explicit Doclist(const RLCSA& _rlcsa);
    virtual ~Doclist();

    inline bool isOk() const { return (this->status == st_ok); }
    inline bool hasDocArray() const { return (this->docarray != 0); }

    virtual void writeTo(const std::string& base_name) const = 0;
    virtual void writeTo(std::ofstream& output) const = 0;
    virtual usint reportSize() const = 0;
    virtual usint reportSizeBrute() const = 0;

    // When docc / ndoc is small, it is faster to reuse the found buffer.
    // This is done by giving an empty WriteBuffer as a parameter.
    // The given buffer will be emptied at the end of the query.
    result_type* listDocuments(const std::string& pattern, found_type* found = 0) const;
    result_type* listDocuments(pair_type range, found_type* found = 0) const;

    // This variant uses brute force.
    result_type* listDocumentsBrute(const std::string& pattern) const;
    result_type* listDocumentsBrute(pair_type range) const;

  protected:
    typedef sdsl::rmq_succinct_sct<true,
            sdsl::bp_support_sada<256,32,sdsl::rank_support_v5<> > > RMQType;

    const RLCSA&     rlcsa;
    CSA::ReadBuffer* docarray;
    RMQType*         rmq;
    status_type      status;

    usint commonStructureSize() const;
    usint commonStructureSizeBrute() const;

    void readRMQ(std::ifstream& input);
    void writeRMQ(std::ofstream& output) const;

    void readDocs(const std::string& base_name);
    void readDocs(std::ifstream& input);
    void writeDocs(const std::string& base_name) const;
    void writeDocs(std::ofstream& output) const;

    virtual pair_type getRMQRange(pair_type sa_range) const = 0;
    virtual usint docAt(usint rmq_index, pair_type sa_range) const = 0;
    virtual void addDocs(usint first_doc, usint rmq_index, result_type* results, found_type* found, pair_type sa_range) const = 0;

  private:
    result_type* listUnsafe(pair_type range, found_type* found) const;
    result_type* listUnsafeBrute(pair_type range) const;

    // These are not allowed.
    Doclist();
    Doclist(const Doclist&);
    Doclist& operator = (const Doclist&);
};


// Sadakane's document listing structure with RMQ using 2n + o(n) bits.
class DoclistSada : public Doclist
{
  public:
    const static std::string EXTENSION;

    explicit DoclistSada(const RLCSA& _rlcsa, bool store_docarray = false);
    DoclistSada(const RLCSA& _rlcsa, std::ifstream& input, bool load_docarray = false);
    DoclistSada(const RLCSA& _rlcsa, const std::string& base_name, bool load_docarray = false);
    virtual ~DoclistSada();

    virtual void writeTo(const std::string& base_name) const;
    virtual void writeTo(std::ofstream& output) const;
    virtual usint reportSize() const;
    virtual usint reportSizeBrute() const;

  protected:
    virtual pair_type getRMQRange(pair_type sa_range) const;
    virtual usint docAt(usint rmq_index, pair_type sa_range) const;
    virtual void addDocs(usint first_doc, usint rmq_index, result_type* results, found_type* found, pair_type sa_range) const;

  private:
    const static usint BLOCK_SIZE = CSA::MEGABYTE;

    void loadFrom(std::ifstream& input);

    // These are not allowed.
    DoclistSada();
    DoclistSada(const DoclistSada&);
    DoclistSada& operator = (const DoclistSada&);
};


// Document listing using RMQ on the run heads of the ILCP array.
class DoclistILCP : public Doclist
{
  public:
    const static std::string EXTENSION;

    explicit DoclistILCP(const RLCSA& _rlcsa);
    DoclistILCP(const RLCSA& _rlcsa, std::ifstream& input, bool load_docarray = false);
    DoclistILCP(const RLCSA& _rlcsa, const std::string& base_name, bool load_docarray = false);
    virtual ~DoclistILCP();

    virtual void writeTo(const std::string& base_name) const;
    virtual void writeTo(std::ofstream& output) const;
    virtual usint reportSize() const;
    virtual usint reportSizeBrute() const;

  protected:
    virtual pair_type getRMQRange(pair_type sa_range) const;
    virtual usint docAt(usint rmq_index, pair_type sa_range) const;
    virtual void addDocs(usint first_doc, usint rmq_index, result_type* results, found_type* found, pair_type sa_range) const;

  private:
    const static usint DV_BLOCK_SIZE = 32;

    CSA::DeltaVector* run_heads;

    void loadFrom(std::ifstream& input);

    // These are not allowed.
    DoclistILCP();
    DoclistILCP(const DoclistILCP&);
    DoclistILCP& operator = (const DoclistILCP&);
};


#endif  // _DOCLIST_DOCLIST_H
