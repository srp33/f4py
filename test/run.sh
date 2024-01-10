#! /bin/bash

set -o errexit

local_dev=no
#local_dev=yes
run_in_background=no
#run_in_background=yes

rm -rf f4
cp -r ../src/f4 .

#######################################################
# Build the Docker image
#######################################################

if [[ "${local_dev}" == "no" ]]
then
  docker build -t srp33/f4_test .
fi

#######################################################
# Run preparatory steps
#######################################################

mkdir -p data

if [[ "${run_in_background}" == "no" ]]
then
  dockerCommand="docker run -i -t --rm --platform linux/x86_64 --user $(id -u):$(id -g) -v $(pwd):/sandbox -v $(pwd)/data:/data -v /tmp:/tmp --workdir=/sandbox srp33/f4_test"
else
  dockerCommand="docker run -d --rm --platform linux/x86_64 --user $(id -u):$(id -g) -v $(pwd):/sandbox -v $(pwd)/data:/data -v /tmp:/tmp --workdir=/sandbox srp33/f4_test"
fi

#$dockerCommand bash -c "time python3 build_tsv.py 10 10 10 8 10000 data/medium.tsv"

#$dockerCommand bash -c "time python3 build_tsv.py 250 250 500 8 1000000 data/large_tall.tsv"
#$dockerCommand bash -c "time python3 build_tsv.py 250000 250000 500000 8 1000 data/large_wide.tsv"
#$dockerCommand bash -c "time python3 build_tsv.py 250 250 500 8 1000000 data/large_tall.tsv.gz"
#$dockerCommand bash -c "time python3 build_tsv.py 250000 250000 500000 8 1000 data/large_wide.tsv.gz"

#$dockerCommand bash -c "time python3 build_tsv_utf8.py 1000000 4 data/test_tall.tsv.gz"
#$dockerCommand bash -c "time python3 build_tsv_utf8.py 2 1000000 data/test_wide.tsv.gz"
#$dockerCommand bash -c "time python3 build_tsv_utf8.py 1000000000 3 data/super_tall.tsv.gz"
#$dockerCommand bash -c "time python3 build_tsv_utf8.py 2 1000000000 data/super_wide.tsv.gz"
#$dockerCommand bash -c "time python3 build_tsv_utf8.py 10000000000 4 data/hyper_tall.tsv.gz"
#$dockerCommand bash -c "time python3 build_tsv_utf8.py 2 10000000000 data/hyper_wide.tsv.gz"

#######################################################
# Run tests
#######################################################

if [[ "${local_dev}" == "yes" ]]
then
  python3 test.py
else
  if [[ "${run_in_background}" == "no" ]]
  then
    $dockerCommand bash -c "python3 test.py"
  else
    echo Saving output to /tmp/f4.out and /tmp/f4.err
    $dockerCommand bash -c "python3 test.py > /tmp/f4.out 2> /tmp/f4.err"
  fi
fi

#######################################################
# Cleanup
#######################################################

rm -rf f4
