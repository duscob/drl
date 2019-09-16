//
// Created by Dustin Cobas Batista<dustin.cobas@gmail.com> on 9/8/19.
//

#include <boost/filesystem.hpp>

#include <gflags/gflags.h>

#include <sdsl/int_vector.hpp>

#include <grammar/re_pair.h>
#include <grammar/slp.h>
#include <grammar/slp_helper.h>
#include <grammar/sampled_slp.h>

#include "drl/io.h"
#include "drl/sa.h"


DEFINE_string(data, "", "Collection file.");
DEFINE_int32(bs, 512, "Block size.");
DEFINE_int32(sf, 4, "Storing factor.");


int main(int argc, char *argv[]) {
  gflags::AllowCommandLineReparsing();
  gflags::ParseCommandLineFlags(&argc, &argv, false);

  if (FLAGS_data.empty()) {
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
  std::cout << std::endl;

  create_directories(coll_name);


  // General data
  sdsl::cache_config cconfig_sep_0{false, coll_name.string(), sdsl::util::basename(data_path.string()) + "_"};


  // RLCSA index
  auto rlcsa = std::make_shared<CSA::RLCSA>(FLAGS_data);
  if (!(rlcsa->isOk())) { return 2; }
  rlcsa->printInfo();
  rlcsa->reportSize(true);

  const auto kNDocs = rlcsa->getNumberOfSequences();


  // DA
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


  //SLP
  grammar::RePairEncoder<true> encoder;

  grammar::SLP<> slp;

  using drl::Load;
  using drl::Save;

  if (!Load(slp, "slp", cconfig_sep_0)) {
    std::cout << "Construct SLP" << std::endl;

    auto wrapper = BuildSLPWrapper(slp);
    encoder.Encode(da.begin(), da.end(), wrapper);

    Save(slp, "slp", cconfig_sep_0);
  }
  const auto kSize_slp = sdsl::size_in_bytes(slp);


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


  grammar::LightSLP<grammar::BasicSLP<sdsl::int_vector<>>,
                    grammar::SampledSLP<>,
                    grammar::Chunks<sdsl::int_vector<>, sdsl::int_vector<>>> lslp_bslp;
  if (!Load(lslp_bslp, slp_filename_prefix, cconfig_sep_0)) {
    std::cout << "Construct LBSLP" << std::endl;

    lslp_bslp = decltype(lslp_bslp)(lslp, bit_compress, bit_compress, bit_compress, bit_compress);

    Save(lslp_bslp, slp_filename_prefix, cconfig_sep_0);
  }
  const auto kSize_lslp_bslp = sdsl::size_in_bytes(lslp_bslp);


  grammar::Chunks<sdsl::int_vector<>, sdsl::int_vector<>> sslp_chunks_bc;
  if (!Load(sslp_chunks_bc, pts_filename_prefix, cconfig_sep_0)) {
    std::cout << "Construct Chunks<BC>" << std::endl;

    sslp_chunks_bc = decltype(sslp_chunks_bc)(cslp_chunks, bit_compress, bit_compress);

    Save(sslp_chunks_bc, pts_filename_prefix, cconfig_sep_0);
  }


  grammar::GCChunks<grammar::SLP<>> sslp_gcchunks;
  if (!Load(sslp_gcchunks, pts_filename_prefix, cconfig_sep_0)) {
    std::cout << "Construct GCChunks" << std::endl;

    const auto &objs = cslp_chunks.GetObjects();
    sslp_gcchunks.Compute(objs.begin(), objs.end(), cslp_chunks, encoder_nslp);

    Save(sslp_gcchunks, pts_filename_prefix, cconfig_sep_0);
  }
  const auto kSize_sslp_gcchunks = sdsl::size_in_bytes(sslp_gcchunks);


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


  return 0;
}