//
// Created by Dustin Cobas<dustin.cobas@gmail.com> on 9/8/19.
//

#ifndef DRL_IO_H_
#define DRL_IO_H_

#include <string>

#include <sdsl/io.hpp>


namespace drl {

template<typename _T>
bool Load(_T &_t, const std::string &_prefix, const sdsl::cache_config &_cconfig) {
  if (sdsl::cache_file_exists<_T>(_prefix, _cconfig)) {
    return sdsl::load_from_file(_t, sdsl::cache_file_name<_T>(_prefix, _cconfig));
  }

  return false;
}


template<typename _T>
bool Save(const _T &_t, const std::string &_prefix, const sdsl::cache_config &_cconfig) {
  return sdsl::store_to_file(_t, sdsl::cache_file_name<_T>(_prefix, _cconfig));
}
}

#endif //DRL_IO_H_
