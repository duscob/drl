
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

# Define helper functions and macros used.
include(cmake/internal_utils.cmake)


include(ConfigGTest)

if (drl_build_tests)
endif ()


if (drl_build_tools)
    cxx_executable_with_flags(build_docs_list "" "gflags;rlcsa;gomp" tool/build_docs_list.cpp)
endif ()


if (drl_build_benchmarks)
endif ()


