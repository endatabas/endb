#!/usr/bin/env bash

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

ENDB_DRIVER_DIR=$(realpath ../clients/python/)
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
