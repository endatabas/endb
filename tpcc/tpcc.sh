#!/usr/bin/env bash

# MIT License

# Copyright (c) 2024 Håkan Råberg and Steven Deobald

# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation files
# (the "Software"), to deal in the Software without restriction,
# including without limitation the rights to use, copy, modify, merge,
# publish, distribute, sublicense, and/or sell copies of the Software,
# and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:

# The above copyright notice and this permission notice (including the
# next paragraph) shall be included in all copies or substantial
# portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
# BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

# Depends on https://github.com/mongodb-labs/py-tpcc
# Which in turn is based on https://github.com/apavlo/py-tpcc/

# Generate configuration:
# ./tpcc.sh --print-config endb > endb.config

# Start endb in another terminal.

# Load data:
# ./tpcc.sh --config=endb.config endb --no-execute --stop-on-error

# Execute:
# ./tpcc.sh --config=endb.config endb --no-load --stop-on-error

cd "$(dirname "$0")"

ENDB_DRIVER_DIR=$(realpath ../examples/)
PYTPCC_PROJECT_DIR=.venv/src/py-tpcc

if [ ! -d .venv ]; then
    python3 -m venv .venv
    . .venv/bin/activate
    pip3 install -r requirements.txt
    cp -a pytpcc $PYTPCC_PROJECT_DIR
else
     . .venv/bin/activate
fi

(cd $PYTPCC_PROJECT_DIR/pytpcc; PYTHONPATH=$ENDB_DRIVER_DIR ./tpcc.py "$@")
