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
##dockerCommand="docker run -d --rm --platform linux/x86_64 --user $(id -u):$(id -g) -v $(pwd):/sandbox -v $(pwd)/data:/data -v /tmp:/tmp --workdir=/sandbox srp33/f4_test"

#$dockerCommand bash -c "time python3 build_tsv.py 10 10 10 10000 data/medium.tsv"

#$dockerCommand bash -c "time python3 build_tsv.py 250 250 500 1000000 data/large_tall.tsv"
#$dockerCommand bash -c "time python3 build_tsv.py 250000 250000 500000 1000 data/large_wide.tsv"

#TODO: Integrate f4 into the analysis paper tests. Check speed and optimize more, if needed.
#        Figure out why it's going so slow in Parser.py to save large files in parallel.
#          Narrow down where the slowness is happening.
#        Make sure we are closing the file handle wherever we are opening it (or find a way to use a with statement).
#          Remove the open_read_file function?
#        Remove lines_per_chunk from Parser? How to deal with read_length?
#        Try potential other speed improvements:
#          * Python 3.11
#          * [Probably not] Try Nuitka? https://nuitka.net (compiles your Python code to C, is supposed to achieve speedups of 3x or greater).
#          * Try PyPi.
#          * Try codon. https://github.com/exaloop/codon
#TODO: Modify class structure for Filters so inheritance is not used.
#TODO: Address remaining TODO items in the code, remove unnecessary commented code.
#TODO: Run this script from beginning to end as a final check.

#######################################################
# Run tests
#######################################################

#python3 test.py
time python3 test.py
#$dockerCommand python3 test.py

#######################################################
# Clean up
#######################################################

rm -rf f4
