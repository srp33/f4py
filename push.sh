#! /bin/bash

set -e errexit

python3 update_version.py

rm -rf dist
python3 -m build
python3 -m twine upload --verbose -u "__token__" dist/*
