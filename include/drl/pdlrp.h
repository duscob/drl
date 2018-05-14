#ifndef _DOCLIST_PDLRP_H
#define _DOCLIST_PDLRP_H

#include "pdltree.h"


class PDLRP
{
  public:
    typedef std::vector<uint>                       result_type;
    typedef enum { st_unfinished, st_error, st_ok } status_type;

    const static std::string EXTENSION;     // .pdlrp
    const static std::string SET_EXTENSION; // .pdlset

    // If compress_sets is true, sets and grammar will be read from the files compressed by irepair.
    PDLRP(const RLCSA& _rlcsa, const std::string& base_name, bool compress_sets, bool use_rpset = false);
    ~PDLRP();

    inline bool isOk() const { return (this->status == st_ok); }

    void writeTo(const std::string& base_name, bool use_rpset = false) const;
    usint reportSize() const;

    // User must delete the result.
    result_type* query(const std::string& pattern) const;
    result_type* query(pair_type sa_range) const;

    usint count(const std::string& pattern) const;
    usint count(pair_type sa_range) const;

  private:
    const RLCSA&     rlcsa;
    status_type      status;

    PDLTree*         tree;
    CSA::ReadBuffer* grammar;
    CSA::MultiArray* blocks;

    result_type* queryUnsafe(pair_type sa_range) const;

    // Returns true if CONTAINS_ALL is encountered.
    bool addBlocks(usint first_block, usint number_of_blocks, result_type* result) const;

    // These are not allowed.
    PDLRP();
    PDLRP(const PDLRP&);
    PDLRP& operator = (const PDLRP&);
};


#endif  // _DOCLIST_PDLRP_H
