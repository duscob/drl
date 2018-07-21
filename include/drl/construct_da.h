//
// Created by Dustin Cobas <dustin.cobas@gmail.com> on 6/2/2018.
//

#ifndef DRL_CONSTRUCT_DA_H
#define DRL_CONSTRUCT_DA_H

#include <sdsl/int_vector_buffer.hpp>


namespace drl {

template<typename II, typename OI, typename DocDelim>
void ConstructDocBorderBasic(II begin, II end, OI oit, const DocDelim &doc_delim) {
  for (auto it = begin; it != end; ++it, ++oit) {
    if (*it == doc_delim) {
      *oit = 1;
    }
  }
}


template<typename II, typename DocBorder, typename DocDelim>
void ConstructDocBorder(II begin, II end, DocBorder &doc_border, const DocDelim &doc_delim, std::size_t size = 0) {
  if (size == 0)
    size = std::distance(begin, end);

  DocBorder tmp_doc_border(size, 0);

  std::size_t i = 0;
  for (auto it = begin; it != end; ++it, ++i) {
    if (*it == doc_delim) {
      tmp_doc_border[i] = 1;
    }
  }

  doc_border.swap(tmp_doc_border);
};


template<uint8_t WIDTH = 8, typename DocBorder, typename DocDelim>
void ConstructDocBorder(const std::string &data_file, DocBorder &doc_border, const DocDelim &doc_delim) {
  sdsl::int_vector_buffer<WIDTH> data_buf(data_file, std::ios::in, 1024 * 1024, WIDTH, true);

  DocBorder tmp_doc_border(data_buf.size(), 0);

  for (std::size_t i = 0; i < data_buf.size(); ++i) {
    if (data_buf[i] == doc_delim) {
      tmp_doc_border[i] = 1;
    }
  }

  doc_border.swap(tmp_doc_border);

  // Previous solution seems to be faster due to iterator comparison.
//  ConstructDocBorder(data_buf.begin(), data_buf.end(), doc_border, doc_delim);
}


template<typename SuffixArray, typename DocBorderRank, typename DocArray>
void ConstructDocArray(const SuffixArray &suf_array, const DocBorderRank &doc_border_rank, DocArray &doc_array) {
  doc_array = DocArray(suf_array.size());
  for (int i = 0; i < suf_array.size(); ++i) {
    doc_array[i] = doc_border_rank(suf_array[i]);
  }
};

}

#endif //DRL_CONSTRUCT_DA_H
