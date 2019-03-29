
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
find_path(CDS_INCLUDE_DIR libcdsBasics.h
        PATH_SUFFIXES cds libcds)
message("CDS_INCLUDE_DIR = ${CDS_INCLUDE_DIR}")

set(COMMON_INCLUDES
        ${PROJECT_SOURCE_DIR}/include
        ${CMAKE_INSTALL_PREFIX}/include
        ${CMAKE_PREFIX_PATH}/include
        ${CDS_INCLUDE_DIR})

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
        include/drl/sa.h
        include/drl/grammar_index.h
        include/drl/dl_basic_scheme.h
        include/drl/dl_sampled_tree_scheme.h)

find_library(RLCSA_LIB rlcsa)
find_library(SDSL_LIB sdsl)
find_library(CDS_LIB cds)
find_library(GRAMMAR_LIB grammar)
find_library(DIVSUFSORT_LIB divsufsort)
find_library(DIVSUFSORT64_LIB divsufsort64)
set(LIBS ${RLCSA_LIB} gomp ${SDSL_LIB} ${CDS_LIB} ${GRAMMAR_LIB} ${DIVSUFSORT_LIB} ${DIVSUFSORT64_LIB})

add_library(drl ${SOURCE_FILES})
target_link_libraries(drl ${LIBS})


# Define helper functions and macros used.
include(cmake/internal_utils.cmake)

find_library(GFLAGS_LIB gflags)

if (drl_build_tests)
    enable_testing()

    include(ConfigGTest)

    cxx_test_with_flags_and_args(construct_da_test "" "gtest;gtest_main;${GFLAGS_LIB};drl;${LIBS}" "" test/construct_da_test.cpp)

    cxx_test_with_flags_and_args(pdloda_test "" "gtest;gtest_main;${GFLAGS_LIB};drl;${LIBS}" "" test/pdloda_test.cpp)
endif ()


if (drl_build_tools)
    cxx_executable_with_flags(build_docs_list "" "${GFLAGS_LIB};${RLCSA_LIB};gomp" tool/build_docs_list.cpp)

    find_package(Boost COMPONENTS filesystem system REQUIRED)

    cxx_executable_with_flags(expand_doc_array "" "${GFLAGS_LIB};${RLCSA_LIB};drl;${Boost_LIBRARIES}" tool/expand_doc_array.cpp)
endif ()


if (drl_build_benchmarks)
    include(ConfigGBenchmark)

    find_package(Boost COMPONENTS filesystem system REQUIRED)

    include_directories(benchmark/r_index)
    cxx_executable_with_flags(query_doc_list_idx_bm "" "${GFLAGS_LIB};benchmark;drl;${LIBS};${Boost_LIBRARIES}" benchmark/query_doc_list_idx_bm.cpp)
endif ()


