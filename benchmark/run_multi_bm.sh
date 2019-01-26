#!/usr/bin/env bash

dir=$1
benchmark=$2
block_size=$3
storing_factor=$4

for coll in `ls $dir`
do
  basename=(`cat $dir/$coll/sample_rates.txt`)
  echo $basename
  
  for bs in `cat $block_size`; do
    for sf in `cat $storing_factor` ; do
      echo "Collection $coll; block-size $bs; storing-factor $sf"
      $benchmark --data $dir/$coll/$basename --patterns $dir/$coll/patterns --bs $bs --sf $sf --benchmark_counters_tabular=true --benchmark_out_format=csv --benchmark_out=$coll-$bs-$sf.csv
    done
  done
done
