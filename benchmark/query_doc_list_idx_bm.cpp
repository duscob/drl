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
#include "drl/dl_basic_scheme.h"
#include "drl/dl_sampled_tree_scheme.h"
#include "drl/helper.h"
#include "drl/pdl_suffix_tree.h"

#include "r_index/r_index.hpp"


DEFINE_string(data, "", "Collection file.");
DEFINE_string(patterns, "", "Patterns file.");
DEFINE_bool(print_size, true, "Print size.");

DEFINE_int32(bs, 512, "Block size.");
DEFINE_int32(sf, 4, "Storing factor.");

auto BM_r_index = [](benchmark::State &st,
                     auto *idx,
                     const auto &doc_border_rank,
                     const auto &patterns,
                     std::size_t _size_in_bytes = 0) {
  usint docc = 0;

  for (auto _ : st) {
    docc = 0;
    for (usint i = 0; i < patterns.size(); i++) {
      auto pat = patterns[i];
      auto occ = idx->locate_all(pat);    //occurrences

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
  if (FLAGS_print_size) st.counters["Size"] = sdsl::size_in_bytes(*idx) + _size_in_bytes;
};

auto BM_dl_brute_da = [](benchmark::State &st,
                         const auto &get_docs,
                         const auto &rlcsa,
                         const auto &patterns,
                         std::size_t _size_in_bytes = 0) {
  usint docc = 0;

  for (auto _ : st) {
    docc = 0;
    for (const auto &pat : patterns) {
      auto range = rlcsa->count(pat);

      std::vector<uint32_t> docs;
      docs.reserve(range.second - range.first + 1);
      auto add_doc = [&docs](auto d) { docs.push_back(d); };

      get_docs(range.first, range.second + 1, add_doc);

      sort(docs.begin(), docs.end());
      docs.erase(unique(docs.begin(), docs.end()), docs.end());

      docc += docs.size();
    }
  }

  st.counters["Patterns"] = patterns.size();
  st.counters["Docs"] = docc;
  if (FLAGS_print_size) st.counters["Size"] = _size_in_bytes;
};

auto BM_dl_scheme =
    [](benchmark::State &st, auto *idx, const auto &rlcsa, const auto &patterns, std::size_t _size_in_bytes = 0) {
      usint docc = 0;

      for (auto _ : st) {
        docc = 0;
        for (const auto &pat : patterns) {
          auto range = rlcsa->count(pat);
          auto res = idx->list(range.first, range.second + 1);
          docc += res.size();
        }
      }

      st.counters["Patterns"] = patterns.size();
      st.counters["Docs"] = docc;
      if (FLAGS_print_size) st.counters["Size"] = _size_in_bytes;
    };

auto BM_query_doc_list_without_buffer =
    [](benchmark::State &st, const auto &idx, const auto &rlcsa, const auto &patterns) {
      if (!(idx->isOk())) {
        st.SkipWithError("Cannot initialize index!");
      }

      usint docc = 0;

      for (auto _ : st) {
        docc = 0;
        for (const auto &pat : patterns) {
          auto range = rlcsa->count(pat);

          auto res = idx->listDocuments(range);

          if (res != nullptr) {
            docc += res->size();
            delete res;
            res = nullptr;
          }
        }
      }

      st.counters["Patterns"] = patterns.size();
      st.counters["Docs"] = docc;
      if (FLAGS_print_size) st.counters["Size"] = idx->reportSize();
    };

auto BM_query_doc_list_with_query = [](benchmark::State &st, const auto &idx, const auto &rlcsa, const auto &patterns) {
  if (!(idx->isOk())) {
    st.SkipWithError("Cannot initialize index!");
  }

  usint docc = 0;

  for (auto _ : st) {
    docc = 0;
    for (const auto &pat : patterns) {
      auto range = rlcsa->count(pat);

      auto res = idx->query(range);

      if (res != nullptr) {
        docc += res->size();
        delete res;
        res = nullptr;
      }
    }
  }

  st.counters["Patterns"] = patterns.size();
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

auto BM_pdloda_rl = [](benchmark::State &st, auto *idx, const auto &rlcsa, const auto &patterns) {
  usint docc = 0;

  for (auto _ : st) {
    docc = 0;
    for (const auto &pat : patterns) {
      auto range = rlcsa->count(pat);
      auto res = idx->SearchInRange(range.first, range.second + 1);
      docc += res.size();
    }
  }

  st.counters["Patterns"] = patterns.size();
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


class MergeSetsBinTreeFunctor {
 public:
  template<typename _II, typename _Sets, typename _Result>
  inline void operator()(_II _first, _II _last, const _Sets &_sets, _Result &_result) const {
    grammar::MergeSetsBinaryTree(_first, _last, _sets, _result);
  }

  template<typename _II, typename _Sets, typename _Result, typename _SetUnion>
  inline void operator()(_II _first,
                         _II _last,
                         const _Sets &_sets,
                         _Result &_result,
                         const _SetUnion &_set_union) const {
    grammar::MergeSetsBinaryTree(_first, _last, _sets, _result, _set_union);
  }
};


class MergeSetsLinearFunctor {
 public:
  template<typename _II, typename _Sets, typename _Result>
  inline void operator()(_II _first, _II _last, const _Sets &_sets, _Result &_result) const {
    auto report = [&_result](const auto &_value) { _result.emplace_back(_value); };

    int c = 1;
    auto prev = _first;
    for (auto it = _first + 1; it != _last; ++it) {
      if (*(it - 1) + 1 == *it) {
        ++c;
      } else {
        _sets.addBlocks(*prev, c, report);
        prev = it;
        c = 1;
      }
    }
    _sets.addBlocks(*prev, c, report);

    sort(_result.begin(), _result.end());
    _result.erase(unique(_result.begin(), _result.end()), _result.end());
  }

//  template<typename _II, typename _Sets, typename _Result, typename _SetUnion>
//  inline void operator()(_II _first, _II _last, const _Sets &_sets, _Result &_result, const _SetUnion &_set_union) const {
//    grammar::MergeSetsBinaryTree(_first, _last, _sets, _result, _set_union);
//  }
};


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

  create_directories(coll_name);



  // RLCSA index
  auto rlcsa = std::make_shared<CSA::RLCSA>(FLAGS_data);
  if (!(rlcsa->isOk())) { return 2; }
  rlcsa->printInfo();
  rlcsa->reportSize(true);

  const auto kNDocs = rlcsa->getNumberOfSequences();



  // Query patterns
  std::vector<std::string> patterns;
  {
    std::ifstream pattern_file(FLAGS_patterns.c_str(), std::ios_base::binary);
    if (!pattern_file) {
      std::cerr << "Error opening pattern file!" << std::endl;
      return 3;
    }
    CSA::readRows(pattern_file, patterns, true);
    pattern_file.close();
  }



  // Counting
  std::vector<CSA::pair_type> ranges;
  ranges.reserve(patterns.size());
  CSA::usint total_size = 0, total_occ = 0;
  double start = CSA::readTimer();
  for (CSA::usint i = 0; i < patterns.size(); i++) {
    CSA::pair_type result = rlcsa->count(patterns[i]);
    total_size += patterns[i].length();
    total_occ += CSA::length(result);
    ranges.push_back(result);
  }
  double seconds = CSA::readTimer() - start;
  double megabytes = total_size / (double) CSA::MEGABYTE;
  std::cout << "Patterns:     " << patterns.size() << " (" << megabytes << " MB)" << std::endl;
  std::cout << "Occurrences:  " << total_occ << std::endl;
  std::cout << "Counting:     " << seconds << " seconds (" << (megabytes / seconds) << " MB/s, "
            << (patterns.size() / seconds) << " patterns/s)" << std::endl;
  std::cout << std::endl;


  // General data
  sdsl::cache_config cconfig_sep_0{false, coll_name.string(), sdsl::util::basename(data_path.string()) + "_"};



  // Get document functionalities
  drl::GetDocRLCSA get_doc_rlcsa(rlcsa);

  drl::RLCSAWrapper rlcsa_wrapper(*rlcsa, data_path.string());
  sdsl::int_vector<> da;
  {
    std::vector<uint32_t> doc_array;
    doc_array.reserve(rlcsa_wrapper.size() + 1);
    rlcsa_wrapper.GetDA(doc_array);

    grammar::Construct(da, doc_array);
    sdsl::util::bit_compress(da);
  }
  const auto kSize_da = sdsl::size_in_bytes(da);

  drl::GetDocDA<decltype(da)> get_doc_da(da);
//  drl::DefaultGetDocs<decltype(get_doc_da)> get_docs_da(get_doc_da);

  grammar::RePairEncoder<true> encoder;

  grammar::SLP<> slp;
  if (!Load(slp, "slp", cconfig_sep_0)) {
    std::cout << "Construct SLP" << std::endl;

    auto wrapper = BuildSLPWrapper(slp);
    encoder.Encode(da.begin(), da.end(), wrapper);

    Save(slp, "slp", cconfig_sep_0);
  }
  const auto kSize_slp = sdsl::size_in_bytes(slp);

  drl::GetDocGCDA<decltype(slp)> get_doc_gcda(slp);



  //*************
  // Brute-Force
  //*************

  // New Brute-Force algorithm using r-index
  sdsl::bit_vector doc_border;
  ri::r_index<> r_idx;
  if (!Load(r_idx, "ri", cconfig_sep_0) || !Load(doc_border, "doc_border", cconfig_sep_0)) {
    std::cout << "Construct RI" << std::endl;

    std::string input;
    {
      std::ifstream fs((coll_path / "data").string());
      std::stringstream buffer;
      buffer << fs.rdbuf();

      input = buffer.str();
    }

    drl::ConstructDocBorder(input.begin(), input.end(), doc_border, '\0');

    std::replace(input.begin(), input.end(), '\0', '\2');
    r_idx = ri::r_index<>(input, false);

    Save(r_idx, "ri", cconfig_sep_0);
    Save(doc_border, "doc_border", cconfig_sep_0);
  }

  sdsl::sd_vector<> doc_border_compact(doc_border);
  auto doc_border_rank = sdsl::sd_vector<>::rank_1_type(&doc_border_compact);

  // BM
  benchmark::RegisterBenchmark("Brute-L",
                               BM_r_index,
                               &r_idx,
                               doc_border_rank,
                               patterns,
                               sdsl::size_in_bytes(doc_border_compact) + sdsl::size_in_bytes(doc_border_rank));

  benchmark::RegisterBenchmark("Brute-D", BM_dl_brute_da, get_doc_da, rlcsa, patterns, kSize_da);

  benchmark::RegisterBenchmark("Brute-C", BM_dl_brute_da, get_doc_gcda, rlcsa, patterns, kSize_slp);



  //**********
  // Sadakane
  //**********

  drl::DefaultRMQ rmq_sada;
  {
    std::ifstream input(FLAGS_data + ".sada");
    rmq_sada.load(input);
  }
  const auto kSize_rmq_sada = sdsl::size_in_bytes(rmq_sada);

  auto sada = drl::BuildDLSadakane<sdsl::bit_vector>(rmq_sada, get_doc_rlcsa, kNDocs + 1);
  benchmark::RegisterBenchmark("SADA-L", BM_dl_scheme, &sada, rlcsa, patterns, kSize_rmq_sada);

  auto sada_da = drl::BuildDLSadakane<sdsl::bit_vector>(rmq_sada, get_doc_da, kNDocs + 1);
  benchmark::RegisterBenchmark("SADA-D", BM_dl_scheme, &sada_da, rlcsa, patterns, kSize_rmq_sada + kSize_da);

  auto sada_gcda = drl::BuildDLSadakane<sdsl::bit_vector>(rmq_sada, get_doc_gcda, kNDocs + 1);
  benchmark::RegisterBenchmark("SADA-C", BM_dl_scheme, &sada_gcda, rlcsa, patterns, kSize_rmq_sada + kSize_slp);



  //******
  // ILCP
  //******

  drl::DefaultRMQ rmq_ilcp;
  std::shared_ptr<CSA::DeltaVector> run_heads_ilcp;
  {
    std::ifstream input(FLAGS_data + ".ilcp");
    rmq_ilcp.load(input);

    run_heads_ilcp.reset(new CSA::DeltaVector(input));
  }
  const auto kSize_rmq_ilcp = sdsl::size_in_bytes(rmq_ilcp) + run_heads_ilcp->reportSize();

  auto ilcp = drl::BuildDLILCP<sdsl::bit_vector>(rmq_ilcp, run_heads_ilcp, get_doc_rlcsa, kNDocs + 1, get_doc_rlcsa);
  benchmark::RegisterBenchmark("ILCP-L", BM_dl_scheme, &ilcp, rlcsa, patterns, kSize_rmq_ilcp);

  auto ilcp_da = drl::BuildDLILCP<sdsl::bit_vector>(rmq_ilcp, run_heads_ilcp, get_doc_da, kNDocs + 1, get_doc_da);
  benchmark::RegisterBenchmark("ILCP-D", BM_dl_scheme, &ilcp_da, rlcsa, patterns, kSize_rmq_ilcp + kSize_da);

  auto ilcp_gcda = drl::BuildDLILCP<sdsl::bit_vector>(rmq_ilcp, run_heads_ilcp, get_doc_gcda, kNDocs + 1, get_doc_gcda);
  benchmark::RegisterBenchmark("ILCP-C", BM_dl_scheme, &ilcp_gcda, rlcsa, patterns, kSize_rmq_ilcp + kSize_slp);



  //********************************
  // Grammar index (Claude & Munro)
  //********************************

  vector<pair<uint *, uint>> queries;
  for (const auto &buf : patterns) {
    if (buf.length() > 0) {
      uint *temp = new uint[buf.length()];
      for (size_t i = 0; i < buf.length(); i++) {
        temp[i] = (uint) (buf[i]);
      }
      queries.emplace_back(std::make_pair(temp, buf.length()));
    }
  }

  GrammarIndex grm_idx((coll_path / "data.grammar").string());

  benchmark::RegisterBenchmark("Grammar", BM_grammar_index, &grm_idx, queries);



  //*****************
  // SLPs and Chunks
  //*****************

  auto prefix = std::to_string(FLAGS_bs) + "-" + std::to_string(FLAGS_sf);
  auto slp_filename_prefix = prefix + "-slp";
  auto pts_filename_prefix = prefix + "-pts";

  grammar::CombinedSLP<> cslp;
  grammar::Chunks<> cslp_chunks;
  if (!Load(cslp, slp_filename_prefix, cconfig_sep_0) || !Load(cslp_chunks, pts_filename_prefix, cconfig_sep_0)) {
    std::cout << "Construct CSLP && CSLPChunks" << std::endl;

    auto wrapper = BuildSLPWrapper(cslp);
    encoder.Encode(da.begin(), da.end(), wrapper);

    auto span_to_big = [](const auto &_set, const auto &_lchildren, const auto &_rchildren) -> bool {
      return 1000 <= _lchildren.size() + _rchildren.size();
    };

    grammar::AddSet<decltype(cslp_chunks)> add_set(cslp_chunks);
//    cslp.Compute(256, add_set, add_set, grammar::MustBeSampled<decltype(cslp_chunks)>(cslp_chunks, 16/*, rlcsa->getNumberOfSequences()*/));
    cslp.Compute(FLAGS_bs, add_set, add_set, grammar::MustBeSampled<decltype(cslp_chunks)>(
        grammar::AreChildrenTooBig<decltype(cslp_chunks)>(cslp_chunks, FLAGS_sf)/*,
        grammar::IsEqual<decltype(cslp_chunks)>(cslp_chunks, 100),
        span_to_big*/));

    Save(cslp, slp_filename_prefix, cconfig_sep_0);
    Save(cslp_chunks, pts_filename_prefix, cconfig_sep_0);
  }
  const auto kSize_cslp = sdsl::size_in_bytes(cslp);

  auto bit_compress = [](sdsl::int_vector<> &_v) { sdsl::util::bit_compress(_v); };
  grammar::RePairEncoder<false> encoder_nslp;
  grammar::LightSLP<> lslp;
  if (!Load(lslp, slp_filename_prefix, cconfig_sep_0)) {
    std::cout << "Construct LSLP" << std::endl;

    lslp.Compute(da.begin(), da.end(), encoder_nslp, cslp);

    Save(lslp, slp_filename_prefix, cconfig_sep_0);
  }
  const auto kSize_lslp = sdsl::size_in_bytes(lslp);

  drl::ExpandSLPFunctor<decltype(lslp)> lslp_get_docs{lslp};

  grammar::LightSLP<grammar::BasicSLP<sdsl::int_vector<>>,
                    grammar::SampledSLP<>,
                    grammar::Chunks<sdsl::int_vector<>, sdsl::int_vector<>>> lslp_bslp;
  if (!Load(lslp_bslp, slp_filename_prefix, cconfig_sep_0)) {
    std::cout << "Construct LBSLP" << std::endl;

    lslp_bslp = decltype(lslp_bslp)(lslp, bit_compress, bit_compress, bit_compress, bit_compress);

    Save(lslp_bslp, slp_filename_prefix, cconfig_sep_0);
  }
  const auto kSize_lslp_bslp = sdsl::size_in_bytes(lslp_bslp);

  drl::ExpandSLPFunctor<decltype(lslp_bslp)> lslp_bslp_get_docs{lslp_bslp};



  //*****
  // PDL
  //*****

  MergeSetsLinearFunctor merge_linear;

  std::shared_ptr<PDLTree> pdl_tree_bc;
  std::shared_ptr<drl::PDLBC<PDLTree>> get_docs_pdl_bc;
  {
    std::ifstream input(FLAGS_data + ".rlcsa.docs", std::ios::binary);

    usint flags = 0;
    input.read((char *) (&flags), sizeof(flags));

    pdl_tree_bc.reset(new PDLTree(*rlcsa, input));

    get_docs_pdl_bc = std::make_shared<drl::PDLBC<PDLTree>>(*pdl_tree_bc, input, flags);
  }
  const auto kSize_pdl_tree_bc = pdl_tree_bc->reportSize();
  const auto kSize_pdl_bc = kSize_pdl_tree_bc + get_docs_pdl_bc->reportSize();

  auto compute_cover_st_bc = drl::BuildComputeCoverSuffixTreeFunctor(*pdl_tree_bc);

  //BM
  benchmark::RegisterBenchmark("PDL-BC", BM_query_doc_list_without_buffer,
                               std::make_shared<CSA::DocArray>(*rlcsa, FLAGS_data), rlcsa, patterns);

  auto dl_pdl_bc_l = drl::BuildDLSampledTreeScheme(compute_cover_st_bc, get_doc_rlcsa, *get_docs_pdl_bc, merge_linear);
  benchmark::RegisterBenchmark("PDL-BC-L", BM_dl_scheme, &dl_pdl_bc_l, rlcsa, patterns, kSize_pdl_bc);

  auto dl_pdl_bc_c =
      drl::BuildDLSampledTreeScheme(compute_cover_st_bc, lslp_bslp_get_docs, *get_docs_pdl_bc, merge_linear);
  benchmark::RegisterBenchmark("PDL-BC-C", BM_dl_scheme, &dl_pdl_bc_c, rlcsa, patterns, kSize_pdl_bc + kSize_lslp_bslp);

  std::shared_ptr<PDLTree> pdl_tree_rp;
  std::shared_ptr<CSA::ReadBuffer> pdl_grammar_rp;
  std::shared_ptr<CSA::MultiArray> pdl_blocks_rp;
  {
    std::ifstream input(FLAGS_data + ".pdlrp", std::ios::binary);

    pdl_tree_rp.reset(new PDLTree(*rlcsa, input));

    pair_type temp;
    input.read((char *) &temp, sizeof(temp)); // Items, bits.
    pdl_grammar_rp.reset(new CSA::ReadBuffer(input, temp.first, temp.second));
    pdl_blocks_rp.reset(CSA::MultiArray::readFrom(input));
  }
  const auto kSize_pdl_tree = pdl_tree_rp->reportSize();
  const auto kSize_pdl_rp = kSize_pdl_tree + pdl_grammar_rp->reportSize() + pdl_blocks_rp->reportSize();

  auto compute_cover_st_rp = drl::BuildComputeCoverSuffixTreeFunctor(*pdl_tree_rp);
  auto get_docs_pdl_rp = drl::BuildGetDocsSuffixTreeRP(*pdl_tree_rp, *pdl_blocks_rp, *pdl_grammar_rp);

  //BM
  benchmark::RegisterBenchmark("PDL-RP", BM_query_doc_list_with_query,
                               std::make_shared<PDLRP>(*rlcsa, FLAGS_data, false), rlcsa, patterns);

  auto dl_pdl_rp_l = drl::BuildDLSampledTreeScheme(compute_cover_st_rp, get_doc_rlcsa, get_docs_pdl_rp, merge_linear);
  benchmark::RegisterBenchmark("PDL-RP-L", BM_dl_scheme, &dl_pdl_rp_l, rlcsa, patterns, kSize_pdl_rp);

  auto dl_pdl_rp_c =
      drl::BuildDLSampledTreeScheme(compute_cover_st_rp, lslp_bslp_get_docs, get_docs_pdl_rp, merge_linear);
  benchmark::RegisterBenchmark("PDL-RP-C", BM_dl_scheme, &dl_pdl_rp_c, rlcsa, patterns, kSize_pdl_rp + kSize_lslp_bslp);



  //*****
  //GCDA
  //*****

  // New algorithm's tests
  drl::ComputeSpanCoverFromBottomFunctor compute_span_cover_from_bottom;
  drl::SAGetDocs sa_docs;
  drl::SLPGetSpan slp_docs;
  drl::LSLPGetSpan lslp_docs;

  // BM
//  auto pdlgt_cslp_chunks = drl::BuildPDLGT(rlcsa_wrapper, cslp, cslp_chunks, compute_span_cover_from_bottom, slp_docs);
//  benchmark::RegisterBenchmark("PDLGT_cslp_chunks", BM_pdloda_rl, &pdlgt_cslp_chunks, ranges);

//  auto pdlgt_sslp_chunks = drl::BuildPDLGT(rlcsa_wrapper, sslp, cslp_chunks, compute_span_cover_from_bottom, sa_docs);
//  benchmark::RegisterBenchmark("PDLGT_sslp_chunks", BM_pdloda_rl, &pdlgt_sslp_chunks, ranges);


  grammar::Chunks<sdsl::int_vector<>, sdsl::int_vector<>> sslp_chunks_bc;
  if (!Load(sslp_chunks_bc, pts_filename_prefix, cconfig_sep_0)) {
    std::cout << "Construct Chunks<BC>" << std::endl;

    sslp_chunks_bc = decltype(sslp_chunks_bc)(cslp_chunks, bit_compress, bit_compress);

    Save(sslp_chunks_bc, pts_filename_prefix, cconfig_sep_0);
  }

  // BM
//  auto pdlgt_cslp_chunks_bc =
//      drl::BuildPDLGT(rlcsa_wrapper, cslp, sslp_chunks_bc, compute_span_cover_from_bottom, slp_docs);
//  benchmark::RegisterBenchmark("PDLGT_cslp_chunks<bc>", BM_pdloda_rl, &pdlgt_cslp_chunks_bc, ranges);


  grammar::GCChunks<grammar::SLP<>> sslp_gcchunks;
  if (!Load(sslp_gcchunks, pts_filename_prefix, cconfig_sep_0)) {
    std::cout << "Construct GCChunks" << std::endl;

    const auto &objs = cslp_chunks.GetObjects();
    sslp_gcchunks.Compute(objs.begin(), objs.end(), cslp_chunks, encoder_nslp);

    Save(sslp_gcchunks, pts_filename_prefix, cconfig_sep_0);
  }
  const auto kSize_sslp_gcchunks = sdsl::size_in_bytes(sslp_gcchunks);

//  // BM
//  auto pdlgt_sslp_gcchunks =
//      drl::BuildPDLGT(rlcsa_wrapper, sslp, sslp_gcchunks, compute_span_cover_from_bottom, sa_docs);
//  benchmark::RegisterBenchmark("PDLGT_sslp_gcchunks", BM_pdloda_rl, &pdlgt_sslp_gcchunks, ranges);

  // BM
  auto pdlgt_cslp_gcchunks =
      drl::BuildPDLGT(rlcsa_wrapper, cslp, sslp_gcchunks, compute_span_cover_from_bottom, slp_docs);
  benchmark::RegisterBenchmark("PDLGT_cslp_gcchunks", BM_pdloda_rl, &pdlgt_cslp_gcchunks, rlcsa, patterns);

  auto compute_cover = drl::BuildComputeCoverBottomFunctor(cslp);
  MergeSetsBinTreeFunctor merge;
  drl::ExpandSLPCoverFunctor<grammar::CombinedSLP<>> slp_get_docs{cslp};

  auto dl_cslp = drl::BuildDLSampledTreeScheme(compute_cover, slp_get_docs, sslp_gcchunks, merge);
  benchmark::RegisterBenchmark("GCDA_cslp_gcchunks",
                               BM_dl_scheme,
                               &dl_cslp,
                               rlcsa,
                               patterns,
                               kSize_cslp + kSize_sslp_gcchunks);

  grammar::GCChunks<
      grammar::SLP<sdsl::int_vector<>, sdsl::int_vector<>>,
      true,
      grammar::Chunks<sdsl::int_vector<>, sdsl::int_vector<>>> sslp_gcchunks_bc;
  if (!Load(sslp_gcchunks_bc, pts_filename_prefix, cconfig_sep_0)) {
    std::cout << "Construct GCChunks<BC>" << std::endl;

    sslp_gcchunks_bc =
        decltype(sslp_gcchunks_bc)(sslp_gcchunks, bit_compress, bit_compress, bit_compress, bit_compress);

    Save(sslp_gcchunks_bc, pts_filename_prefix, cconfig_sep_0);
  }
  const auto kSize_sslp_gcchunks_bc = sdsl::size_in_bytes(sslp_gcchunks_bc);


  // BM
  auto pdlgt_cslp_gcchunks_bc =
      drl::BuildPDLGT(rlcsa_wrapper, cslp, sslp_gcchunks_bc, compute_span_cover_from_bottom, slp_docs);
  benchmark::RegisterBenchmark("PDLGT_cslp_gcchunks<bc>", BM_pdloda_rl, &pdlgt_cslp_gcchunks_bc, rlcsa, patterns);

  auto dl_cslp_bc = drl::BuildDLSampledTreeScheme(compute_cover, slp_get_docs, sslp_gcchunks_bc, merge);
  benchmark::RegisterBenchmark("GCDA_cslp_gcchunks<bc>",
                               BM_dl_scheme,
                               &dl_cslp_bc,
                               rlcsa,
                               patterns,
                               kSize_cslp + kSize_sslp_gcchunks_bc);


  // BM
  auto pdlgt_lslp_gcchunks_bc =
      drl::BuildPDLGT(rlcsa_wrapper, lslp, sslp_gcchunks_bc, compute_span_cover_from_bottom, lslp_docs);
  benchmark::RegisterBenchmark("PDLGT_lslp_gcchunks<bc>", BM_pdloda_rl, &pdlgt_lslp_gcchunks_bc, rlcsa, patterns);

  auto dl_lslp = drl::BuildDLSampledTreeScheme(compute_cover, lslp_get_docs, sslp_gcchunks_bc, merge);
  benchmark::RegisterBenchmark("GCDA_lslp_gcchunks<bc>",
                               BM_dl_scheme,
                               &dl_lslp,
                               rlcsa,
                               patterns,
                               kSize_lslp + kSize_sslp_gcchunks_bc);


  // BM
  auto pdlgt_lslp_bslp_gcchunks_bc =
      drl::BuildPDLGT(rlcsa_wrapper, lslp_bslp, sslp_gcchunks_bc, compute_span_cover_from_bottom, lslp_docs);
  benchmark::RegisterBenchmark("PDLGT_lslp<bslp>_gcchunks<bc>",
                               BM_pdloda_rl,
                               &pdlgt_lslp_bslp_gcchunks_bc,
                               rlcsa,
                               patterns);

  auto dl_lslp_bslp = drl::BuildDLSampledTreeScheme(compute_cover, lslp_bslp_get_docs, sslp_gcchunks_bc, merge);
  benchmark::RegisterBenchmark("GCDA_lslp<bslp>_gcchunks<bc>",
                               BM_dl_scheme,
                               &dl_lslp_bslp,
                               rlcsa,
                               patterns,
                               kSize_lslp_bslp + kSize_sslp_gcchunks_bc);


  grammar::GCChunks<
      grammar::BasicSLP<sdsl::int_vector<>>,
      true,
      grammar::Chunks<sdsl::int_vector<>, sdsl::int_vector<>>> sslp_gcchunks_bslp_bc;
  if (!Load(sslp_gcchunks_bslp_bc, pts_filename_prefix, cconfig_sep_0)) {
    std::cout << "Construct GCChunks<BSLP,BC>" << std::endl;

    sslp_gcchunks_bslp_bc =
        decltype(sslp_gcchunks_bslp_bc)(sslp_gcchunks, bit_compress, bit_compress, bit_compress, bit_compress);

    Save(sslp_gcchunks_bslp_bc, pts_filename_prefix, cconfig_sep_0);
  }
  const auto kSize_sslp_gcchunks_bslp_bc = sdsl::size_in_bytes(sslp_gcchunks_bslp_bc);

  // BM
  auto pdlgt_lslp_bslp_gcchunks_bslp_bc =
      drl::BuildPDLGT(rlcsa_wrapper, lslp_bslp, sslp_gcchunks_bslp_bc, compute_span_cover_from_bottom, lslp_docs);
  benchmark::RegisterBenchmark("PDLGT_lslp<bslp>_gcchunks<bslp,bc>",
                               BM_pdloda_rl,
                               &pdlgt_lslp_bslp_gcchunks_bslp_bc,
                               rlcsa,
                               patterns);

  auto dl_lslp_bslp_bslp =
      drl::BuildDLSampledTreeScheme(compute_cover, lslp_bslp_get_docs, sslp_gcchunks_bslp_bc, merge);
  benchmark::RegisterBenchmark("GCDA_lslp<bslp>_gcchunks<bslp,bc>",
                               BM_dl_scheme,
                               &dl_lslp_bslp_bslp,
                               rlcsa,
                               patterns,
                               kSize_lslp_bslp + kSize_sslp_gcchunks_bslp_bc);

  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();

  return 0;
}
