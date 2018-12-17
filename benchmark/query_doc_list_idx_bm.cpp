//
// Created by Dustin Cobas Batista <dustin.cobas@gmail.com> on 14-05-18.
//


#include <iostream>

#include <boost/filesystem.hpp>

#include <gflags/gflags.h>
#include <benchmark/benchmark.h>

#include <rlcsa/rlcsa.h>
#include <rlcsa/docarray.h>

#include <sdsl/suffix_arrays.hpp>
#include <sdsl/suffix_array_algorithm.hpp>

#include <grammar/slp.h>
#include <grammar/slp_metadata.h>

#include "drl/doclist.h"
#include "drl/pdlrp.h"
#include "drl/grammar_index.h"
#include "drl/pdloda.h"
#include "drl/sa.h"

#include "r_index/r_index.hpp"


DEFINE_string(data, "", "Collection file.");
DEFINE_string(patterns, "", "Patterns file.");
DEFINE_bool(print_size, true, "Print size.");

auto BM_query_doc_list_brute_force_sa = [](benchmark::State &st, const auto &sa, const auto &ranges) {
  usint docc = 0;

  for (auto _ : st) {
    docc = 0;
    for (usint i = 0; i < ranges.size(); i++) {
      usint *docs = sa->locate(ranges[i]);
      if (docs != nullptr) {
        usint temp = CSA::length(ranges[i]);
        sa->getSequenceForPosition(docs, temp);
        CSA::sequentialSort(docs, docs + temp);
        docc += std::unique(docs, docs + temp) - docs;
        delete docs;
        docs = nullptr;
      }
    }
  }

  st.counters["Patterns"] = ranges.size();
  st.counters["Docs"] = docc;
  if (FLAGS_print_size) st.counters["Size"] = 0;
};

auto BM_query_doc_list_brute_force_da = [](benchmark::State &st, const auto &idx, const auto &ranges) {
  if (!(idx->isOk())) {
    st.SkipWithError("Cannot initialize index!");
  }

  usint docc = 0;

  for (auto _ : st) {
    docc = 0;
    for (usint i = 0; i < ranges.size(); i++) {
      auto res = idx->listDocumentsBrute(ranges[i]);
      if (res != nullptr) {
        docc += res->size();
        delete res;
        res = nullptr;
      }
    }
  }

  st.counters["Patterns"] = ranges.size();
  st.counters["Docs"] = docc;
  if (FLAGS_print_size) st.counters["Size"] = idx->reportSize();
};

auto BM_query_doc_list = [](benchmark::State &st, const auto &idx, const auto &rlcsa, const auto &ranges) {
  if (!(idx->isOk())) {
    st.SkipWithError("Cannot initialize index!");
  }

  usint docc = 0;

  for (auto _ : st) {
    docc = 0;
    Doclist::found_type found(rlcsa->getNumberOfSequences(), 1);
    for (usint i = 0; i < ranges.size(); i++) {
      auto res = idx->listDocuments(ranges[i], &found);
      if (res != nullptr) {
        docc += res->size();
        delete res;
        res = nullptr;
      }
    }
  }

  st.counters["Patterns"] = ranges.size();
  st.counters["Docs"] = docc;
  if (FLAGS_print_size) st.counters["Size"] = idx->reportSize();
};

auto BM_query_doc_list_without_buffer = [](benchmark::State &st, const auto &idx, const auto &ranges) {
  if (!(idx->isOk())) {
    st.SkipWithError("Cannot initialize index!");
  }

  usint docc = 0;

  for (auto _ : st) {
    docc = 0;
    for (usint i = 0; i < ranges.size(); i++) {
      auto res = idx->listDocuments(ranges[i]);
      if (res != nullptr) {
        docc += res->size();
        delete res;
        res = nullptr;
      }
    }
  }

  st.counters["Patterns"] = ranges.size();
  st.counters["Docs"] = docc;
  if (FLAGS_print_size) st.counters["Size"] = idx->reportSize();
};


auto BM_query_doc_list_with_query = [](benchmark::State &st, const auto &idx, const auto &ranges) {
  if (!(idx->isOk())) {
    st.SkipWithError("Cannot initialize index!");
  }

  usint docc = 0;

  for (auto _ : st) {
    docc = 0;
    for (usint i = 0; i < ranges.size(); i++) {
      auto res = idx->query(ranges[i]);
      if (res != nullptr) {
        docc += res->size();
        delete res;
        res = nullptr;
      }
    }
  }

  st.counters["Patterns"] = ranges.size();
  st.counters["Docs"] = docc;
  if (FLAGS_print_size) st.counters["Size"] = idx->reportSize();
};


auto BM_grammar_index = [](benchmark::State &st, auto *idx, const auto &queries) {
  usint docc = 0;

  for (auto _ : st) {
    docc = 0;
    for (usint i = 0; i < queries.size(); i++) {
      auto res = idx->List(queries[i].first, queries[i].second);
      if (res != nullptr) {
        docc += res->size();
        delete res;
        res = nullptr;
      }
    }
  }

  st.counters["Patterns"] = queries.size();
  st.counters["Docs"] = docc;
  if (FLAGS_print_size) st.counters["Size"] = idx->GetSize();
};


auto BM_r_index = [](benchmark::State &st, auto *idx, const auto &doc_border_rank, const auto &patterns) {
  usint docc = 0;

  for (auto _ : st) {
    docc = 0;
    for (usint i = 0; i < patterns.size(); i++) {
      auto pat = patterns[i];
      auto occ = idx->locate_all(pat);	//occurrences

      std::vector<uint32_t> docs;
      docs.reserve(occ.size());

      for (const auto &item : occ) {
        docs.push_back(doc_border_rank(item));
      }

      sort(docs.begin(), docs.end());
      docs.erase(unique(docs.begin(), docs.end()), docs.end());

      docc += docs.size();
    }
  }

  st.counters["Patterns"] = patterns.size();
  st.counters["Docs"] = docc;
  if (FLAGS_print_size) st.counters["Size"] = sdsl::size_in_bytes(*idx);
};


auto BM_pdloda = [](benchmark::State &st, auto *idx, const auto &ranges) {
  usint docc = 0;

  for (auto _ : st) {
    docc = 0;
    for (usint i = 0; i < ranges.size(); i++) {
//      std::vector<std::size_t> res;
      auto res = idx->SearchInRange(ranges[i].first, ranges[i].second/*, std::back_inserter(res)*/);
      docc += res.size();
    }
  }

  st.counters["Patterns"] = ranges.size();
  st.counters["Docs"] = docc;
};

auto BM_pdloda_rl = [](benchmark::State &st, auto *idx, const auto &ranges) {
  usint docc = 0;

  for (auto _ : st) {
    docc = 0;
    for (usint i = 0; i < ranges.size(); i++) {
//      std::vector<std::size_t> res;
      auto res = idx->SearchInRange(ranges[i].first, ranges[i].second + 1/*, std::back_inserter(res)*/);
      docc += res.size();
    }
  }

  st.counters["Patterns"] = ranges.size();
  st.counters["Docs"] = docc;
  if (FLAGS_print_size) st.counters["Size"] = sdsl::size_in_bytes(*idx);
};


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


int main(int argc, char *argv[]) {
  gflags::AllowCommandLineReparsing();
  gflags::ParseCommandLineFlags(&argc, &argv, false);

  if (FLAGS_data.empty() || FLAGS_patterns.empty()) {
    std::cerr << "Command-line error!!!" << std::endl;
    return 1;
  }

  boost::filesystem::path data_path = FLAGS_data;
  auto coll_path = data_path.parent_path();
  auto coll_name = coll_path.filename();

  std::cout << "Base name: " << FLAGS_data << std::endl;
  std::cout << "Data path: " << data_path << std::endl;
  std::cout << "Collection path: " << coll_path << std::endl;
  std::cout << "Collection name: " << coll_name << std::endl;
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

  benchmark::RegisterBenchmark("Brute-L", BM_query_doc_list_brute_force_sa, rlcsa, ranges);

  benchmark::RegisterBenchmark("Brute-D", BM_query_doc_list_brute_force_da,
                               std::make_shared<DoclistSada>(*rlcsa, FLAGS_data, true), ranges);

  benchmark::RegisterBenchmark("ILCP-L", BM_query_doc_list, std::make_shared<DoclistILCP>(*rlcsa, FLAGS_data), rlcsa,
                               ranges);

  benchmark::RegisterBenchmark("ILCP-D", BM_query_doc_list, std::make_shared<DoclistILCP>(*rlcsa, FLAGS_data, true),
                               rlcsa, ranges);

  benchmark::RegisterBenchmark("PDL-BC", BM_query_doc_list_without_buffer,
                               std::make_shared<CSA::DocArray>(*rlcsa, FLAGS_data), ranges);

  benchmark::RegisterBenchmark("PDL-RP", BM_query_doc_list_with_query,
                               std::make_shared<PDLRP>(*rlcsa, FLAGS_data, false),
                               ranges);

//  benchmark::RegisterBenchmark("PDL-set", BM_query_doc_list_with_query,
//                               std::make_shared<PDLRP>(*rlcsa, FLAGS_data, false, true),
//                               ranges);

  benchmark::RegisterBenchmark("SADA-L", BM_query_doc_list, std::make_shared<DoclistSada>(*rlcsa, FLAGS_data), rlcsa,
                               ranges);

  benchmark::RegisterBenchmark("SADA-D", BM_query_doc_list, std::make_shared<DoclistSada>(*rlcsa, FLAGS_data, true),
                               rlcsa, ranges);

  /// Grammar index (Claude & Munro)
  vector<pair<uint *, uint>> queries;
  for (const auto &buf : rows) {
    if (buf.length() > 0) {
      uint *temp = new uint[buf.length()];
      for (size_t i = 0; i < buf.length(); i++) {
        temp[i] = (uint) (buf[i]);
      }
      queries.push_back(std::make_pair(temp, buf.length()));
    }
  }

  GrammarIndex grm_idx((coll_path / "data.grammar").string());

  benchmark::RegisterBenchmark("Grammar", BM_grammar_index, &grm_idx, queries);



  create_directories(coll_name);

//  std::vector<CSA::pair_type> ranges1(rows.size());
//  auto file_1_sep = coll_path / "data.1";
//
//  std::string id = sdsl::util::basename(file_1_sep.string()) + "_";
//  sdsl::cache_config cconfig_sep_1{false, coll_name.string(), id};
//  drl::CSAWrapper<sdsl::csa_bitcompressed<>> csa(file_1_sep.string(), (uint8_t) 1, cconfig_sep_1);
//
//  for (CSA::usint i = 0; i < rows.size(); i++) {
//    const auto &sa = csa.GetSA();
//    if (0 < backward_search(sa, 0, sa.size() - 1, rows[i].begin(), rows[i].end(), ranges1[i].first, ranges1[i].second))
//      ++ranges1[i].second;
//  }

  sdsl::cache_config cconfig_sep_0{false, coll_name.string(), sdsl::util::basename(data_path.string()) + "_"};
  drl::RLCSAWrapper rlcsa_wrapper(*rlcsa, data_path.string());
//  std::cout << "|RLCSAWrapper| = " << rlcsa_wrapper.size() << std::endl;

  // New Brute-Force algorithm using r-index
  std::string input;

  {
    std::ifstream fs((coll_path / "data").string());
    std::stringstream buffer;
    buffer << fs.rdbuf();

    input = buffer.str();
  }

  sdsl::bit_vector doc_border;
  drl::ConstructDocBorder(input.begin(), input.end(), doc_border, '\0');
  auto doc_border_rank = sdsl::bit_vector::rank_1_type(&doc_border);

  std::cout << "DocBorderRank = " << doc_border_rank(input.size()) << std::endl;

  ri::r_index<> r_idx;

  if (!Load(r_idx, "ri", cconfig_sep_0)) {
    std::cout << "Construct RI" << std::endl;

    std::replace(input.begin(), input.end(), '\0', '\2');
    r_idx = ri::r_index<>(input, false);

    Save(r_idx, "ri", cconfig_sep_0);
  }

  // BM
  benchmark::RegisterBenchmark("RIndex", BM_r_index, &r_idx, doc_border_rank, rows);



  // New algorithm's tests
  grammar::RePairEncoder<true> encoder;
  std::vector<uint32_t> doc_array;
  doc_array.reserve(rlcsa_wrapper.size() + 1);
  rlcsa_wrapper.GetDA(doc_array);

  drl::ComputeSpanCoverFromTopFunctor compute_span_cover_from_top;
  drl::ComputeSpanCoverFromBottomFunctor compute_span_cover_from_bottom;
  drl::SAGetDocs sa_docs;
  drl::SLPGetSpan slp_docs;
  drl::LSLPGetSpan lslp_docs;

  grammar::SLP<> slp;
  if (!Load(slp, "slp", cconfig_sep_0)) {
    std::cout << "Construct SLP" << std::endl;

    auto wrapper = BuildSLPWrapper(slp);
    encoder.Encode(doc_array.begin(), doc_array.end(), wrapper);

    Save(slp, "slp", cconfig_sep_0);
  }

  auto bit_compress = [](sdsl::int_vector<> &_v) { sdsl::util::bit_compress(_v); };

//  grammar::SLP<sdsl::int_vector<>, sdsl::int_vector<>> slp_bc;
//  if (!Load(slp_bc, "slp", cconfig_sep_0)) {
//    std::cout << "Construct SLP<BC>" << std::endl;
//
//    slp_bc = decltype(slp_bc)(slp, bit_compress, bit_compress);
//
//    Save(slp_bc, "slp", cconfig_sep_0);
//  }
//
//  grammar::SampledPTS<grammar::SLP<sdsl::int_vector<>, sdsl::int_vector<>>> spts_slp_bc;
//  if (Load(spts_slp_bc, "pts", cconfig_sep_0)) {
//    spts_slp_bc.SetSLP(&slp_bc);
//  } else {
//    std::cout << "Construct SPTS" << std::endl;
//
//    spts_slp_bc.Compute(&slp_bc);
//
//    Save(spts_slp_bc, "pts", cconfig_sep_0);
//  }
//
//  // BM
//  auto pdlgt_slp_bc_spts = drl::BuildPDLGT(rlcsa_wrapper, slp_bc, spts_slp_bc, compute_span_cover_from_top, sa_docs);
//  benchmark::RegisterBenchmark("PDLGT_slp<bc>_spts", BM_pdloda_rl, &pdlgt_slp_bc_spts, ranges);


  grammar::CombinedSLP<> cslp;
  grammar::Chunks<> cslp_chunks;
  if (!Load(cslp, "slp", cconfig_sep_0) || !Load(cslp_chunks, "pts", cconfig_sep_0)) {
    std::cout << "Construct CSLP && CSLPChunks" << std::endl;

    auto wrapper = BuildSLPWrapper(cslp);
    encoder.Encode(doc_array.begin(), doc_array.end(), wrapper);

    auto span_to_big = [](const auto &_set, const auto &_lchildren, const auto &_rchildren) -> bool {
      return 1000 <= _lchildren.size() + _rchildren.size();
    };


    grammar::AddSet<decltype(cslp_chunks)> add_set(cslp_chunks);
//    cslp.Compute(256, add_set, add_set, grammar::MustBeSampled<decltype(cslp_chunks)>(cslp_chunks, 16/*, rlcsa->getNumberOfSequences()*/));
    cslp.Compute(256, add_set, add_set, grammar::MustBeSampled<decltype(cslp_chunks)>(
        grammar::AreChildrenTooBig<decltype(cslp_chunks)>(cslp_chunks, 16),
        grammar::IsEqual<decltype(cslp_chunks)>(cslp_chunks, 100)/*,
        span_to_big*/));

    Save(cslp, "slp", cconfig_sep_0);
    Save(cslp_chunks, "pts", cconfig_sep_0);
  }

  // BM
  auto pdlgt_cslp_chunks = drl::BuildPDLGT(rlcsa_wrapper, cslp, cslp_chunks, compute_span_cover_from_bottom, slp_docs);
  benchmark::RegisterBenchmark("PDLGT_cslp_chunks", BM_pdloda_rl, &pdlgt_cslp_chunks, ranges);


//  grammar::SampledSLP<> sslp;
////  grammar::Chunks<> sslp_chunks;
//  if (!Load(sslp, "slp", cconfig_sep_0)/* || !Load(sslp_chunks, "pts", cconfig_sep_0)*/) {
////    std::cout << "Construct SSLP && SSLPChunks" << std::endl;
//    std::cout << "Construct SSLP" << std::endl;
//
//    sslp = cslp;
////    grammar::AddSet<decltype(sslp_chunks)> add_set(sslp_chunks);
////    sslp.Compute(slp, 256, add_set, add_set, grammar::MustBeSampled<decltype(sslp_chunks)>(sslp_chunks, 16));
//
//    Save(sslp, "slp", cconfig_sep_0);
////    Save(sslp_chunks, "pts", cconfig_sep_0);
//  }
//
//  // BM
//  auto pdlgt_sslp_chunks = drl::BuildPDLGT(rlcsa_wrapper, sslp, cslp_chunks, compute_span_cover_from_bottom, sa_docs);
//  benchmark::RegisterBenchmark("PDLGT_sslp_chunks", BM_pdloda_rl, &pdlgt_sslp_chunks, ranges);


  grammar::Chunks<sdsl::int_vector<>, sdsl::int_vector<>> sslp_chunks_bc;
  if (!Load(sslp_chunks_bc, "pts", cconfig_sep_0)) {
    std::cout << "Construct Chunks<BC>" << std::endl;

    sslp_chunks_bc = decltype(sslp_chunks_bc)(cslp_chunks, bit_compress, bit_compress);

    Save(sslp_chunks_bc, "pts", cconfig_sep_0);
  }

  // // BM
  // auto pdlgt_sslp_chunks_bc =
  //     drl::BuildPDLGT(rlcsa_wrapper, sslp, sslp_chunks_bc, compute_span_cover_from_bottom, sa_docs);
  // benchmark::RegisterBenchmark("PDLGT_sslp_chunks<bc>", BM_pdloda_rl, &pdlgt_sslp_chunks_bc, ranges);

  // BM
  auto pdlgt_cslp_chunks_bc =
      drl::BuildPDLGT(rlcsa_wrapper, cslp, sslp_chunks_bc, compute_span_cover_from_bottom, slp_docs);
  benchmark::RegisterBenchmark("PDLGT_cslp_chunks<bc>", BM_pdloda_rl, &pdlgt_cslp_chunks_bc, ranges);

  grammar::RePairEncoder<false> encoder_nslp;
  grammar::GCChunks<grammar::SLP<>> sslp_gcchunks;
  if (!Load(sslp_gcchunks, "pts", cconfig_sep_0)) {
    std::cout << "Construct GCChunks" << std::endl;

    const auto &objs = cslp_chunks.GetObjects();
    sslp_gcchunks.Compute(objs.begin(), objs.end(), cslp_chunks, encoder_nslp);

    Save(sslp_gcchunks, "pts", cconfig_sep_0);
  }

//  // BM
//  auto pdlgt_sslp_gcchunks =
//      drl::BuildPDLGT(rlcsa_wrapper, sslp, sslp_gcchunks, compute_span_cover_from_bottom, sa_docs);
//  benchmark::RegisterBenchmark("PDLGT_sslp_gcchunks", BM_pdloda_rl, &pdlgt_sslp_gcchunks, ranges);

  // BM
  auto pdlgt_cslp_gcchunks =
      drl::BuildPDLGT(rlcsa_wrapper, cslp, sslp_gcchunks, compute_span_cover_from_bottom, slp_docs);
  benchmark::RegisterBenchmark("PDLGT_cslp_gcchunks", BM_pdloda_rl, &pdlgt_cslp_gcchunks, ranges);

  grammar::GCChunks<
      grammar::SLP<sdsl::int_vector<>, sdsl::int_vector<>>,
      true,
      grammar::Chunks<sdsl::int_vector<>, sdsl::int_vector<>>> sslp_gcchunks_bc;
  if (!Load(sslp_gcchunks_bc, "pts", cconfig_sep_0)) {
    std::cout << "Construct GCChunks<BC>" << std::endl;

    sslp_gcchunks_bc =
        decltype(sslp_gcchunks_bc)(sslp_gcchunks, bit_compress, bit_compress, bit_compress, bit_compress);

    Save(sslp_gcchunks_bc, "pts", cconfig_sep_0);
  }


//  // BM
//  auto pdlgt_sslp_gcchunks_bc =
//      drl::BuildPDLGT(rlcsa_wrapper, sslp, sslp_gcchunks_bc, compute_span_cover_from_bottom, sa_docs);
//  benchmark::RegisterBenchmark("PDLGT_sslp_gcchunks<bc>", BM_pdloda_rl, &pdlgt_sslp_gcchunks_bc, ranges);

  // BM
  auto pdlgt_cslp_gcchunks_bc =
      drl::BuildPDLGT(rlcsa_wrapper, cslp, sslp_gcchunks_bc, compute_span_cover_from_bottom, slp_docs);
  benchmark::RegisterBenchmark("PDLGT_cslp_gcchunks<bc>", BM_pdloda_rl, &pdlgt_cslp_gcchunks_bc, ranges);


  grammar::LightSLP<> lslp;
  if (!Load(lslp, "slp", cconfig_sep_0)) {
    std::cout << "Construct LSLP" << std::endl;

    lslp.Compute(doc_array.begin(), doc_array.end(), encoder_nslp, cslp);

    Save(lslp, "slp", cconfig_sep_0);
  }


  // BM
  auto pdlgt_lslp_gcchunks_bc =
      drl::BuildPDLGT(rlcsa_wrapper, lslp, sslp_gcchunks_bc, compute_span_cover_from_bottom, lslp_docs);
  benchmark::RegisterBenchmark("PDLGT_lslp_gcchunks<bc>", BM_pdloda_rl, &pdlgt_lslp_gcchunks_bc, ranges);


  grammar::LightSLP<grammar::BasicSLP<sdsl::int_vector<>>,
                    grammar::SampledSLP<>,
                    grammar::Chunks<sdsl::int_vector<>, sdsl::int_vector<>>> lslp_bslp;
  if (!Load(lslp_bslp, "slp", cconfig_sep_0)) {
    std::cout << "Construct LBSLP" << std::endl;

    lslp_bslp = decltype(lslp_bslp)(lslp, bit_compress, bit_compress, bit_compress, bit_compress);

    Save(lslp_bslp, "slp", cconfig_sep_0);
  }

  // BM
  auto pdlgt_lslp_bslp_chunks =
      drl::BuildPDLGT(rlcsa_wrapper, lslp_bslp, cslp_chunks, compute_span_cover_from_bottom, lslp_docs);
  benchmark::RegisterBenchmark("PDLGT_lslp<bslp>_chunks", BM_pdloda_rl, &pdlgt_lslp_bslp_chunks, ranges);

  // BM
  auto pdlgt_lslp_bslp_chunks_bc =
      drl::BuildPDLGT(rlcsa_wrapper, lslp_bslp, sslp_chunks_bc, compute_span_cover_from_bottom, lslp_docs);
  benchmark::RegisterBenchmark("PDLGT_lslp<bslp>_chunks<bc>", BM_pdloda_rl, &pdlgt_lslp_bslp_chunks_bc, ranges);

  // BM
  auto pdlgt_lslp_bslp_gcchunks_bc =
      drl::BuildPDLGT(rlcsa_wrapper, lslp_bslp, sslp_gcchunks_bc, compute_span_cover_from_bottom, lslp_docs);
  benchmark::RegisterBenchmark("PDLGT_lslp<bslp>_gcchunks<bc>", BM_pdloda_rl, &pdlgt_lslp_bslp_gcchunks_bc, ranges);

  grammar::GCChunks<
      grammar::BasicSLP<sdsl::int_vector<>>,
      true,
      grammar::Chunks<sdsl::int_vector<>, sdsl::int_vector<>>> sslp_gcchunks_bslp_bc;
  if (!Load(sslp_gcchunks_bslp_bc, "pts", cconfig_sep_0)) {
    std::cout << "Construct GCChunks<BSLP,BC>" << std::endl;

    sslp_gcchunks_bslp_bc =
        decltype(sslp_gcchunks_bslp_bc)(sslp_gcchunks, bit_compress, bit_compress, bit_compress, bit_compress);

    Save(sslp_gcchunks_bslp_bc, "pts", cconfig_sep_0);
  }

  // BM
  auto pdlgt_lslp_bslp_gcchunks_bslp_bc =
      drl::BuildPDLGT(rlcsa_wrapper, lslp_bslp, sslp_gcchunks_bslp_bc, compute_span_cover_from_bottom, lslp_docs);
  benchmark::RegisterBenchmark("PDLGT_lslp<bslp>_gcchunks<bslp,bc>",
                               BM_pdloda_rl,
                               &pdlgt_lslp_bslp_gcchunks_bslp_bc,
                               ranges);

  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();

  return 0;
}
