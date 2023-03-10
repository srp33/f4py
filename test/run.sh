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

#TODO: Use underscores consistently or don't use them at all. Not necessary because __init__.py hides most functions.
#TODO: Reduce the imports to just use the specific functions we need?
#TODO: By default, num_cols_per_chunk is the number of columns divided by the number of threads + 1?
#TODO: By default, num_rows_per_write is the number of rows divided by the number of threads + 1?
#TODO: Integrate f4 into the analysis paper tests. Check speed and optimize more, if needed.
#        See if there's a way to speed up Parallel, delayed.
#          Change from joblib to asyncio.
#TODO: Remove class structure for Filters so object orientation is not used.
#TODO: Address remaining TODO items in the code, remove unnecessary commented code.
#TODO: Try potential speed improvements:
#        - Python 3.11
#        - [Probably not] Try Nuitka? https://nuitka.net (compiles your Python code to C, is supposed to achieve speedups of 3x or greater).
#        - [Probably not] Parallelize the process of generating the output file (save to temp file)? This *might* help when doing a lot of decompression.
#TODO: Run this script from beginning to end as a final check.

#######################################################
# Run tests
#######################################################

python3 test.py
#$dockerCommand python3 test.py

#######################################################
# Clean up
#######################################################

rm -rf f4
