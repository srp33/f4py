#! /bin/bash

set -o errexit

#######################################################
# Build the Docker image
#######################################################

#docker build --platform linux/x86_64 -t srp33/f4_test .

#######################################################
# Run preparatory steps
#######################################################

mkdir -p data

rm -rf f4
cp -r ../src/f4 .

dockerCommand="docker run -i -t --rm --platform linux/x86_64 --user $(id -u):$(id -g) -v $(pwd):/sandbox -v $(pwd)/data:/data -v /tmp:/tmp --workdir=/sandbox srp33/f4_test"

#$dockerCommand bash -c "time python3 build_tsv.py 10 10 10 10000 data/medium.tsv"

#$dockerCommand bash -c "time python3 build_tsv.py 250 250 500 1000000 data/large_tall.tsv.gz"
#$dockerCommand bash -c "time python3 build_tsv.py 250000 250000 500000 1000 data/large_wide.tsv.gz"

#$dockerCommand bash -c "time python3 build_tsv.py 2 0 0 1000000000 data/super_tall.tsv.gz"
#$dockerCommand bash -c "time python3 build_tsv.py 1000000000 0 0 2 data/super_wide.tsv.gz"

#######################################################
# Run tests
#######################################################

python3 test.py
#time python3 test.py
#$dockerCommand python3 test.py

#######################################################
# Clean up
#######################################################

rm -rf f4
