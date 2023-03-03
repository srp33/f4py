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

#TODO: See if there's a way to speed up Parallel, delayed.
#TODO:   Change from joblib to multiprocessing?
#          https://superfastpython.com/multiprocessing-for-loop/
#          https://docs.python.org/3.7/library/multiprocessing.html
#TODO: Reduce the Parser imports to just use the specific functions we need.
#TODO: Integrate f4 into the analysis paper tests. Check speed and optimize more, if needed.
#TODO: Cache the cc coordinates in a dictionary within Parser and no longer store ll information?
#        See _parse_data_coords() function.
#TODO: Remove class structure so object orientation is not used.
#TODO: Address remaining TODO items in the code, remove unnecessary commented code.
#TODO: Try potential speed improvements:
#        - Python 3.11
#        - [Probably not] Try Nuitka? https://nuitka.net (compiles your Python code to C, is supposed to achieve speedups of 3x or greater).
#        - [Probably not] Parallelize the process of generating the output file (save to temp file)? This *might* help when doing a lot of decompression.
#TODO: Run this script from beginning to end as a final check.

#######################################################
# Run tests
#######################################################

#python3 test.py
time python3 test.py
#time python3 delete_me.py
#$dockerCommand python3 test.py

#######################################################
# Clean up
#######################################################

rm -rf f4
