#!/bin/bash
# 010_get_data.sh
# 2024-06-24 | CR
# Run get_data.py to prepare the data directory
#
set -e
REPO_BASEDIR="`pwd`"
cd "`dirname "$0"`" ;
SCRIPTS_DIR="`pwd`" ;
cd "${SCRIPTS_DIR}/get_data"
python3 -m venv venv
source venv/bin/activate
if [ -f "requirements.txt" ]; then
    pip install -r requirements.txt
fi
python3 -m get_data
deactivate
# rm -rf venv
