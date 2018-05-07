
option(drl_build_tests "Build all of drl's tests." ON)

option(drl_build_tools "Build all of drl's tools." ON)

option(drl_build_benchmarks "Build all of drl's benchmarks." ON)

cmake_minimum_required(VERSION 2.8)


##Project Information
project(drl CXX C)




if (drl_build_tests)
endif ()


if (drl_build_tools)
endif ()


if (drl_build_benchmarks)
endif ()


