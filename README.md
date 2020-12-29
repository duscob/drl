DRL - Document Retrieval Library
=========

DRL is a C++ library containing Document Listing indexes for repetitive scenarios. Currently, it contains solutions for the following problem:

>**Document listing**: Given a document collection and a pattern, return the set of documents where the pattern occurs.

The implemented indexes are described in:

>Cobas D, Navarro G. Fast, Small, and Simple Document Listing on
Repetitive Text Collections. In Proceedings of the SPIRE 2019: 26th International Symposium on String Processing and Information Retrieval.


Getting Started
-----

### Compiling
DRL is a C++ template library using standard C++14.
As a build system, the project uses [CMake](https://cmake.org/).

>##### Dependencies
>
>DRL requires the following external libraries:
>  * [Succinct Data Structure Library (SDSL)](https://github.com/simongog/sdsl-lite "SDSL's GitHub repository")
>  * [Grammar Library](https://github.com/duscob/grammar "Grammar's GitHub repository")
>  * [RLCSA](http://jltsiren.kapsi.fi/rlcsa "RLCSA's Webpage")
>  * [CDS Library](https://github.com/fclaude/libcds "CDS's GitHub repository")
>
>All libraries must be installed on your system.
> Our project uses CMake to find the required dependencies.

>###### RLCSA
> For `RLCSA` library, you must copy the headers and library in your `<install-path>` using the next steps.
>   * Headers: Copy header files to directory `<install-path>/include/rlcsa/*`. You must also copy the header files in internal directories (`rlcsa/bits` and `rlcsa/misc`) recursively keeping the directories.
>   * Library: Copy library `rlcsa.a` to `<install-path>/lib/librlcsa.a`. CMake uses prefix `lib` to find libraries, so library filename must be changed.
> 
> Notice that `<install-path>` could be any path used by CMake to find dependencies (we recommend `/usr/local`).


##### Building

To build our solution, you can create a build folder on the source directory and move to it.
```shell
$ cd dret
$ mkdir build
$ cd build
```

Then build and compile the project using the commands `cmake` and `make`, respectively.
```shell
$ cmake ..
$ make
```



### Tools and Benchmarks

We include a tool to preprocess the datasets, build the document retrieval indexes, and benchmark them.
To facilitate the building process, we also provide some shell scripts to orchestrate the flow execution.

>##### Dependencies
>
>Our tools and benchmarks also require that the [GFlags](https://gflags.github.io/gflags/) library is installed on your system.


```shell
$ <source_dir>/benchmark/run_bm.sh <datasets_dir> 
```

where
* *datasets_dir* is the parent directory of the datasets. Each subdirectory is a different dataset containing two files: `data` and `patterns`. The file `data` is a binary file that represents the collection, where each document is separated by the symbol **0** (zero). The file must not contain symbols **1** and **2**. In addition, each subdirectory must contain the precomputed external indexes.

All paths must be absolute.

This script creates a folder for each dataset in the current directory and stores on it the data structures required by each index (if they do not exist).
Besides, the script execute a benchmark to compare all the indexes.
