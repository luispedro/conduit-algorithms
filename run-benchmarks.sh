#!/usr/bin/env bash
set -e

shopt -s nullglob
mkdir -p test_data

(for i in $(seq 1000); do for j in $(seq 1000); do echo $RANDOM; done ; done)> test_data/input.txt
gzip < test_data/input.txt > test_data/input.txt.gz
bzip2 < test_data/input.txt > test_data/input.txt.bz2

exec stack bench --benchmark-arguments "--output bench.html"
