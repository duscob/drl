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


DEFINE_string(data, "", "Collection file.");
DEFINE_string(patterns, "", "Patterns file.");


auto BM_query_doc_list_brute_force_sa = [](benchmark::State &st, const auto &sa, const auto &ranges) {
    for (auto _ : st) {
        usint docc = 0;
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
};


auto BM_query_doc_list_brute_force_da = [](benchmark::State &st, const auto &idx, const auto &ranges) {
    if (!(idx->isOk())) {
        st.SkipWithError("Cannot initialize index!");
    }

    for (auto _ : st) {
        usint docc = 0;
        for (usint i = 0; i < ranges.size(); i++) {
            auto res = idx->listDocumentsBrute(ranges[i]);
            if (res != nullptr) {
                docc += res->size();
                delete res;
                res = nullptr;
            }
        }
    }
};


auto BM_query_doc_list = [](benchmark::State &st, const auto &idx, const auto &rlcsa, const auto &ranges) {
    if (!(idx->isOk())) {
        st.SkipWithError("Cannot initialize index!");
    }

    for (auto _ : st) {
        Doclist::found_type found(rlcsa->getNumberOfSequences(), 1);
        usint docc = 0;
        for (usint i = 0; i < ranges.size(); i++) {
            auto res = idx->listDocuments(ranges[i], &found);
            if (res != nullptr) {
                docc += res->size();
                delete res;
                res = nullptr;
            }
        }
    }
};


auto BM_query_doc_list_without_buffer = [](benchmark::State &st, const auto &idx, const auto &ranges) {
    if (!(idx->isOk())) {
        st.SkipWithError("Cannot initialize index!");
    }

    for (auto _ : st) {
        usint docc = 0;
        for (usint i = 0; i < ranges.size(); i++) {
            auto res = idx->listDocuments(ranges[i]);
            if (res != nullptr) {
                docc += res->size();
                delete res;
                res = nullptr;
            }
        }
    }
};


auto BM_query_doc_list_with_query = [](benchmark::State &st, const auto &idx, const auto &ranges) {
    if (!(idx->isOk())) {
        st.SkipWithError("Cannot initialize index!");
    }

    for (auto _ : st) {
        usint docc = 0;
        for (usint i = 0; i < ranges.size(); i++) {
            auto res = idx->query(ranges[i]);
            if (res != nullptr) {
                docc += res->size();
                delete res;
                res = nullptr;
            }
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


    benchmark::Initialize(&argc, argv);
    benchmark::RunSpecifiedBenchmarks();

    return 0;
}