//
// Created by Dustin Cobas <dustin.cobas@gmail.com> on 07-05-18.
//

#include <gflags/gflags.h>

#include <rlcsa/rlcsa.h>


DEFINE_string(data, "", "Base name");
DEFINE_string(patterns, "", "Pattern file");


int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    if (FLAGS_data.empty() || FLAGS_patterns.empty()) {
        std::cerr << "Input Error!!!" << std::endl;
        return 1;
    }

    // Index
    CSA::RLCSA rlcsa(FLAGS_data);
    if (!(rlcsa.isOk())) {
        return 2;
    }
//    rlcsa.printInfo();
//    rlcsa.reportSize(true);


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
        CSA::pair_type result = rlcsa.count(rows[i]);
        total_size += rows[i].length();
        total_occ += CSA::length(result);
        ranges.push_back(result);
    }
    double seconds = CSA::readTimer() - start;
    double megabytes = total_size / (double) CSA::MEGABYTE;
//    std::cout << "Patterns:     " << rows.size() << " (" << megabytes << " MB)" << std::endl;
//    std::cout << "Occurrences:  " << total_occ << std::endl;
//    std::cout << "Counting:     " << seconds << " seconds (" << (megabytes / seconds) << " MB/s, "
//              << (rows.size() / seconds) << " patterns/s)" << std::endl;
//    std::cout << std::endl;


    //Document listing using brute force
    CSA::usint docc = 0;
    for (CSA::usint i = 0; i < rows.size(); i++) {
        std::cout << rows[i] << std::endl;
        CSA::usint *docs = rlcsa.locate(ranges[i]);
        if (docs != nullptr) {
            CSA::usint temp = CSA::length(ranges[i]);
            rlcsa.getSequenceForPosition(docs, temp);
            CSA::sequentialSort(docs, docs + temp);
            auto udocs = std::unique(docs, docs + temp) - docs;
            docc += udocs;

            for (int j = 0; j < udocs; ++j) {
                std::cout << docs[j] << " ";
            }
            std::cout << std::endl;
            delete docs;
            docs = nullptr;
        }
    }
    std::cout << std::endl;
}
