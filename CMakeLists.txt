
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
        include/drl/sa.h)

find_library(RLCSA_LIB rlcsa)
find_library(SDSL_LIB sdsl)
find_library(GRAMMAR_LIB grammar)
set(LIBS ${RLCSA_LIB} gomp ${SDSL_LIB} ${GRAMMAR_LIB})

add_library(drl ${SOURCE_FILES})
target_link_libraries(drl ${LIBS})


# Define helper functions and macros used.
include(cmake/internal_utils.cmake)

find_library(GFLAGS_LIB gflags)

if (drl_build_tests)
    enable_testing()

    include(ConfigGTest)

    cxx_test_with_flags_and_args(construct_da_test "" "gtest;gtest_main;${GFLAGS_LIB};drl;divsufsort;divsufsort64" "" test/construct_da_test.cpp)

    cxx_test_with_flags_and_args(pdloda_test "" "gtest;gtest_main;${GFLAGS_LIB};drl;divsufsort;divsufsort64" "" test/pdloda_test.cpp)
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
    cxx_executable_with_flags(query_doc_list_idx_bm "" "${GFLAGS_LIB};benchmark;drl;${SDSL_LIB};divsufsort;divsufsort64;${Boost_LIBRARIES}" benchmark/query_doc_list_idx_bm.cpp)
endif ()


