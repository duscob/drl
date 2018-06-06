//
// Created by Dustin Cobas <dustin.cobas@gmail.com> on 6/2/2018.
//

#ifndef DRL_CONSTRUCT_DA_H
#define DRL_CONSTRUCT_DA_H

#include <sdsl/int_vector_buffer.hpp>

namespace drl{

template <uint8_t WIDTH = 8, typename DocBorder, typename DocDelim>
void ConstructDocBorder(const std::string& data_file, DocBorder &doc_border, const DocDelim &doc_delim) {
  sdsl::int_vector_buffer<WIDTH> data_buf(data_file, std::ios::in, 1024*1024, WIDTH, true);

  DocBorder tmp_doc_border(data_buf.size(), 0);

  for (std::size_t i = 0; i < data_buf.size(); ++i) {
    if (data_buf[i] == doc_delim) {
      tmp_doc_border[i] = 1;
    }
  }

  doc_border.swap(tmp_doc_border);
}


template <typename SuffixArray, typename DocBorderRank, typename DocArray>
void ConstructDocArray(const SuffixArray &suf_array, const DocBorderRank &doc_border_rank, DocArray &doc_array){
  doc_array = DocArray(suf_array.size());
  for (int i = 0; i < suf_array.size(); ++i) {
    doc_array[i] = doc_border_rank(suf_array[i]);
  }
};

}

#endif //DRL_CONSTRUCT_DA_H
