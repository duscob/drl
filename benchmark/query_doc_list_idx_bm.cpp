//
// Created by Dustin Cobas Batista <dustin.cobas@gmail.com> on 14-05-18.
//


#include <iostream>

#include <gflags/gflags.h>
#include <benchmark/benchmark.h>

#include <rlcsa/rlcsa.h>
#include <rlcsa/docarray.h>

#include "drl/doclist.h"
#include "drl/pdlrp.h"

#include "drl/pdloda.h"

#include <grammar/slp.h>
#include <grammar/slp_metadata.h>
#include <sdsl/csa_wt.hpp>
#include <sdsl/csa_bitcompressed.hpp>
#include <sdsl/suffix_array_algorithm.hpp>


DEFINE_string(data, "", "Collection file.");
DEFINE_string(patterns, "", "Patterns file.");

auto BM_query_doc_list_brute_force_sa = [](benchmark::State &st, const auto &sa, const auto &ranges) {
  uint64_t d;
  for (auto _ : st) {
    usint docc = 0;
    for (usint i = 0; i < ranges.size(); i++) {
      usint *docs = sa->locate(ranges[i]);
      if (docs != nullptr) {
        usint temp = CSA::length(ranges[i]);
        sa->getSequenceForPosition(docs, temp);
        CSA::sequentialSort(docs, docs + temp);
        d = std::unique(docs, docs + temp) - docs;
        docc += d;
        delete docs;
        docs = nullptr;
      }
    }
  }
  std::cout << d << std::endl;
};

auto BM_query_doc_list_brute_force_da = [](benchmark::State &st, const auto &idx, const auto &ranges) {
  if (!(idx->isOk())) {
    st.SkipWithError("Cannot initialize index!");
  }

  uint64_t d;
  for (auto _ : st) {
    usint docc = 0;
    for (usint i = 0; i < ranges.size(); i++) {
      auto res = idx->listDocumentsBrute(ranges[i]);
      if (res != nullptr) {
        d = res->size();
        docc += res->size();
        delete res;
        res = nullptr;
      }
    }
  }
  std::cout << d << std::endl;
};

auto BM_query_doc_list = [](benchmark::State &st, const auto &idx, const auto &rlcsa, const auto &ranges) {
  if (!(idx->isOk())) {
    st.SkipWithError("Cannot initialize index!");
  }

  uint64_t d;
  for (auto _ : st) {
    Doclist::found_type found(rlcsa->getNumberOfSequences(), 1);
    usint docc = 0;
    for (usint i = 0; i < ranges.size(); i++) {
      auto res = idx->listDocuments(ranges[i], &found);
      if (res != nullptr) {
        d = res->size();
        docc += res->size();
        delete res;
        res = nullptr;
      }
    }
  }
  std::cout << d << std::endl;
};

auto BM_query_doc_list_without_buffer = [](benchmark::State &st, const auto &idx, const auto &ranges) {
  if (!(idx->isOk())) {
    st.SkipWithError("Cannot initialize index!");
  }

  uint64_t d;
  for (auto _ : st) {
    usint docc = 0;
    for (usint i = 0; i < ranges.size(); i++) {
      auto res = idx->listDocuments(ranges[i]);
      if (res != nullptr) {
        d = res->size();
        docc += res->size();
        delete res;
        res = nullptr;
      }
    }
  }
  std::cout << d << std::endl;
};

auto BM_query_doc_list_with_query = [](benchmark::State &st, const auto &idx, const auto &ranges) {
  if (!(idx->isOk())) {
    st.SkipWithError("Cannot initialize index!");
  }

  uint64_t d;
  for (auto _ : st) {
    usint docc = 0;
    for (usint i = 0; i < ranges.size(); i++) {
      auto res = idx->query(ranges[i]);
      if (res != nullptr) {
        d = res->size();
        docc += res->size();
        delete res;
        res = nullptr;
      }
    }
  }
  std::cout << d << std::endl;
//  auto res = idx->query(ranges[0]);
//  std::cout << "[";
//  for (auto &&item : *res) {
//    std::cout << item << ";";
//  }
//  std::cout << "]" << std::endl;
//  if (res != nullptr) {
//    delete res;
//    res = nullptr;
//  }
};

auto BM_pdloda = [](benchmark::State &st, auto *idx, const auto &ranges) {
  uint64_t d;
  for (auto _ : st) {
    usint docc = 0;
    for (usint i = 0; i < ranges.size(); i++) {
//      std::vector<std::size_t> res;
      auto res = idx->SearchInRange(ranges[i].first, ranges[i].second/*, std::back_inserter(res)*/);
      d = res.size();
      docc += res.size();
    }
  }

  std::cout << d << std::endl;
//  {
//    std::vector<std::size_t> res;
//    idx->SearchInRange(ranges[0].first, ranges[0].second, std::back_inserter(res));
//    std::cout << "[";
//    for (auto &&item : res) {
//      std::cout << item - 1 << ";";
//    }
//    std::cout << "]" << std::endl;
//  }
//  {
//    std::string pat = "MDKSE";
//    std::vector<std::size_t> res;
//    idx->Search(pat.begin(), pat.end(), std::back_inserter(res));
//    std::cout << "[";
//    for (auto &&item : res) {
//      std::cout << item << ";";
//    }
//    std::cout << "]" << std::endl;
//  }
};

auto BM_pdloda_span_cover = [](benchmark::State &st, auto *idx, const auto &ranges) {
  uint64_t d;
  for (auto _ : st) {
    usint docc = 0;
    for (usint i = 0; i < ranges.size(); i++) {
      std::vector<std::size_t> res;
      idx->SpanCover(ranges[i].first, ranges[i].second);
      d = res.size();
      docc += res.size();
    }
  }
};


int main(int argc, char *argv[]) {
  gflags::AllowCommandLineReparsing();
  gflags::ParseCommandLineFlags(&argc, &argv, false);

  if (FLAGS_data.empty() || FLAGS_patterns.empty()) {
    std::cerr << "Command-line error!!!" << std::endl;
    return 1;
  }

  std::cout << "Base name: " << FLAGS_data << std::endl;
  std::cout << "Patterns: " << FLAGS_patterns << std::endl;
  std::cout << std::endl;

  // Index
  auto rlcsa = std::make_shared<CSA::RLCSA>(FLAGS_data);
  if (!(rlcsa->isOk())) { return 2; }
  rlcsa->printInfo();
  rlcsa->reportSize(true);

  // Patterns
  std::ifstream pattern_file(FLAGS_patterns.c_str(), std::ios_base::binary);
  if (!pattern_file) {
    std::cerr << "Error opening pattern file!" << std::endl;
    return 3;
  }
  std::vector<std::string> rows;
  CSA::readRows(pattern_file, rows, true);
  pattern_file.close();

  // Counting
  std::vector<CSA::pair_type> ranges;
  ranges.reserve(rows.size());
  CSA::usint total_size = 0, total_occ = 0;
  double start = CSA::readTimer();
  for (CSA::usint i = 0; i < rows.size(); i++) {
    CSA::pair_type result = rlcsa->count(rows[i]);
    total_size += rows[i].length();
    total_occ += CSA::length(result);
    ranges.push_back(result);
  }
  double seconds = CSA::readTimer() - start;
  double megabytes = total_size / (double) CSA::MEGABYTE;
  std::cout << "Patterns:     " << rows.size() << " (" << megabytes << " MB)" << std::endl;
  std::cout << "Occurrences:  " << total_occ << std::endl;
  std::cout << "Counting:     " << seconds << " seconds (" << (megabytes / seconds) << " MB/s, "
            << (rows.size() / seconds) << " patterns/s)" << std::endl;
  std::cout << std::endl;
//    writeRanges(ranges, FLAGS_patterns + ".ranges");

  benchmark::RegisterBenchmark("Brute-L", BM_query_doc_list_brute_force_sa, rlcsa, ranges);

  benchmark::RegisterBenchmark("Brute-D", BM_query_doc_list_brute_force_da,
                               std::make_shared<DoclistSada>(*rlcsa, FLAGS_data, true), ranges);

  benchmark::RegisterBenchmark("ILCP-L", BM_query_doc_list, std::make_shared<DoclistILCP>(*rlcsa, FLAGS_data), rlcsa,
                               ranges);

  benchmark::RegisterBenchmark("ILCP-D", BM_query_doc_list, std::make_shared<DoclistILCP>(*rlcsa, FLAGS_data, true),
                               rlcsa, ranges);

  benchmark::RegisterBenchmark("PDL", BM_query_doc_list_without_buffer,
                               std::make_shared<CSA::DocArray>(*rlcsa, FLAGS_data), ranges);

  benchmark::RegisterBenchmark("PDL-RP", BM_query_doc_list_with_query,
                               std::make_shared<PDLRP>(*rlcsa, FLAGS_data, false),
                               ranges);

  benchmark::RegisterBenchmark("PDL-set", BM_query_doc_list_with_query,
                               std::make_shared<PDLRP>(*rlcsa, FLAGS_data, false, true),
                               ranges);

  benchmark::RegisterBenchmark("SADA-L", BM_query_doc_list, std::make_shared<DoclistSada>(*rlcsa, FLAGS_data), rlcsa,
                               ranges);

  benchmark::RegisterBenchmark("SADA-D", BM_query_doc_list, std::make_shared<DoclistSada>(*rlcsa, FLAGS_data, true),
                               rlcsa, ranges);


  std::vector<CSA::pair_type> ranges1(rows.size());
  std::cout << "range1" << std::endl;

  drl::PDLODA<sdsl::csa_bitcompressed<>, grammar::SLP<>, grammar::PTS<>, drl::ComputeSpanCoverFromTopWrapper> idx1(
      "/home/dcobas/Workspace/PhD/Research/Document_Retrieval/Data/preprocessed/data.1",
      '\x01',
      drl::ConstructSLPAndComputePTS());

  std::cout << "idx1 ended" << std::endl;

  for (CSA::usint i = 0; i < rows.size(); i++) {
    const auto &sa = idx1.sa();
    if (0 < backward_search(sa, 0, sa.size() - 1, rows[i].begin(), rows[i].end(), ranges1[i].first, ranges1[i].second))
      ++ranges1[i].second;
//    std::cout << "  " << rows[i] << ":[" << ranges1[i].first << ";" << ranges1[i].second << "]" << std::endl;
  }

  idx1.SpanCover(ranges1.front().first, ranges1.front().second);

  benchmark::RegisterBenchmark("PDLODA_Full_SpanCover", BM_pdloda_span_cover, &idx1, ranges1);

  benchmark::RegisterBenchmark("PDLODA_full", BM_pdloda, &idx1, ranges1);

  drl::PDLODA<sdsl::csa_bitcompressed<>,
              grammar::SLP<>,
              grammar::SampledPTS<grammar::SLP<>>,
              drl::ComputeSpanCoverFromTopWrapper> idx2(
      "/home/dcobas/Workspace/PhD/Research/Document_Retrieval/Data/preprocessed/data.1",
      '\x01',
      drl::ConstructSLPAndComputePTS(),
      drl::LoadSLPAndPTS());

  std::cout << "idx2 ended" << std::endl;

  idx2.SpanCover(ranges1.front().first, ranges1.front().second);

  benchmark::RegisterBenchmark("PDLODA_sampled_top_SpanCover", BM_pdloda_span_cover, &idx2, ranges1);

  benchmark::RegisterBenchmark("PDLODA_sampled_top", BM_pdloda, &idx2, ranges1);

  drl::PDLODA<sdsl::csa_bitcompressed<>,
              grammar::SampledSLP<>,
              grammar::Chunks<>,
              drl::ComputeSpanCoverFromBottomWrapper> idx3(
      "/home/dcobas/Workspace/PhD/Research/Document_Retrieval/Data/preprocessed/data.1",
      '\x01',
      drl::ComputeSLPAndPTS());

  std::cout << "idx3 ended" << std::endl;

  idx3.SpanCover(ranges1.front().first, ranges1.front().second);

  benchmark::RegisterBenchmark("PDLODA_sampled_bottom_SpanCover", BM_pdloda_span_cover, &idx3, ranges1);

  benchmark::RegisterBenchmark("PDLODA_sampled_bottom", BM_pdloda, &idx3, ranges1);


  drl::PDLODA<sdsl::csa_bitcompressed<>,
              grammar::SampledSLP<>,
              grammar::Chunks<sdsl::int_vector<>, sdsl::int_vector<>>,
              drl::ComputeSpanCoverFromBottomWrapper> idx4(
      "/home/dcobas/Workspace/PhD/Research/Document_Retrieval/Data/preprocessed/data.1",
      '\x01',
      drl::ComputeSLPAndActionPTS());

  std::cout << "idx4 ended" << std::endl;

  idx4.SpanCover(ranges1.front().first, ranges1.front().second);

  benchmark::RegisterBenchmark("PDLODA_sampled_bottom_SpanCover_compact", BM_pdloda_span_cover, &idx4, ranges1);

  benchmark::RegisterBenchmark("PDLODA_sampled_bottom_compact", BM_pdloda, &idx4, ranges1);


  drl::PDLODA<sdsl::csa_bitcompressed<>,
              grammar::SampledSLP<>,
              grammar::GrammarCompressedChunks<grammar::SLP<>>,
              drl::ComputeSpanCoverFromBottomWrapper> idx5(
      "/home/dcobas/Workspace/PhD/Research/Document_Retrieval/Data/preprocessed/data.1",
      '\x01',
      drl::ComputeSLPAndCompactPTS());
  std::cout << "idx5 ended" << std::endl;

//  idx5.SpanCover(ranges1.front().first, ranges1.front().second);

//  benchmark::RegisterBenchmark("PDLODA_sampled_bottom_SpanCover_compact_PTS", BM_pdloda_span_cover, &idx5, ranges1);

  benchmark::RegisterBenchmark("PDLODA_sampled_bottom_compact_PTS", BM_pdloda, &idx5, ranges1);

  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();

  return 0;
}