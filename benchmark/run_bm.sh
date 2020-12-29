#!/usr/bin/env bash

dir=$1
benchmark=${2:-"$SCRIPT_DIR/../build/query_doc_list_idx_bm"}

for coll in `ls $dir`
do
  echo "Collection $coll"

  basename=(`cat $dir/$coll/sample_rates.txt`)
  echo $basename

  $benchmark --data $dir/$coll/$basename --patterns $dir/$coll/patterns --benchmark_counters_tabular=true --benchmark_out_format=csv --benchmark_out=$coll.csv
done
