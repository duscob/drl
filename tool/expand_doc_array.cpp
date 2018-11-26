//
// Created by Dustin Cobas Batista <dustin.cobas@gmail.com> on 9/16/18.
//

#include <iostream>

#include <boost/filesystem/path.hpp>
#include <boost/filesystem/operations.hpp>

#include <gflags/gflags.h>

#include <rlcsa/rlcsa.h>
#include <rlcsa/docarray.h>

#include "drl/utils.h"


DEFINE_string(data, "", "Collection basename file.");
//DEFINE_string(da, "", "Document Array. Sequence of documents if with size = log(d)");
//DEFINE_string(out, "", "Document Array. Sequence of documents if with size = log(d)");


int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  gflags::ParseCommandLineFlags(&argc, &argv, true);


  if (FLAGS_data.empty()) {
    std::cerr << "Input Error!!!" << std::endl;
    return 1;
  }

  boost::filesystem::path data_path = FLAGS_data;
//  auto coll_path = data_path.parent_path();
//  auto coll_name = coll_path.filename();

  CSA::RLCSA rlcsa(data_path.string());
  auto da = readDocArray(rlcsa, data_path.string());

//  create_directories(coll_name);

  auto da_file = /*coll_name / */data_path.filename();
  da_file += ".int.docs";
  std::cout << da_file << std::endl;
  std::ofstream output(da_file.string(), std::ios_base::binary);

  da->goToItem(0);
  while (da->hasNextItem()) {
    uint32_t doc = da->nextItem();
    output.write((char *) &doc, sizeof(doc));
  }

  delete da;

  return 0;
}