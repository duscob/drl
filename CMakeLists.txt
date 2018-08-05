
option(drl_build_tests "Build all of drl's tests." ON)

option(drl_build_tools "Build all of drl's tools." ON)

option(drl_build_benchmarks "Build all of drl's benchmarks." ON)

cmake_minimum_required(VERSION 2.8)


##Project Information
project(drl CXX C)

# Location of additional cmake modules
set(CMAKE_MODULE_PATH
        ${CMAKE_MODULE_PATH}
        ${PROJECT_SOURCE_DIR}/cmake)

# Guard against in-source builds and bad build-type strings
include(ConfigSafeGuards)


#Global Setup
set(CMAKE_CXX_STANDARD 14)


# Set common include folder for module
set(COMMON_INCLUDES ${PROJECT_SOURCE_DIR}/include)

include_directories(${COMMON_INCLUDES})


set(SOURCE_FILES include/drl/doclist.h
        src/doclist.cpp
        include/drl/utils.h
        src/utils.cpp
        include/drl/pdlrp.h
        src/pdlrp.cpp
        include/drl/pdltree.h
        src/pdltree.cpp
        include/drl/construct_da.h
        include/drl/pdloda.h
        include/drl/merge_sets.h)

set(LIBS rlcsa gomp sdsl grammar)

add_library(drl ${SOURCE_FILES})
target_link_libraries(drl ${LIBS})


# Define helper functions and macros used.
include(cmake/internal_utils.cmake)


if (drl_build_tests)
    enable_testing()

    include(ConfigGTest)

    cxx_test_with_flags_and_args(construct_da_test "" "gtest;gtest_main;gflags;drl;divsufsort;divsufsort64" "" test/construct_da_test.cpp)

    cxx_test_with_flags_and_args(merge_sets_test "" "gtest;gtest_main;gflags;drl;divsufsort;divsufsort64" "" test/merge_sets_test.cpp)

    cxx_test_with_flags_and_args(pdloda_test "" "gtest;gtest_main;gflags;drl;divsufsort;divsufsort64" "" test/pdloda_test.cpp)
endif ()


if (drl_build_tools)
    cxx_executable_with_flags(build_docs_list "" "gflags;rlcsa;gomp" tool/build_docs_list.cpp)
endif ()


if (drl_build_benchmarks)
    include(ConfigGBenchmark)

    cxx_executable_with_flags(query_doc_list_idx_bm "" "gflags;benchmark;drl;sdsl;divsufsort;divsufsort64" benchmark/query_doc_list_idx_bm.cpp)
endif ()


