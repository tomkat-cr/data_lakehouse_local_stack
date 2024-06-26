#!/bin/bash
# 020_get_pokemon_data.sh
# 2024-06-24 | CR
# Run get_pokemon_data.py to prepare the data directory
#
set -e
# Repository root directory
REPO_BASEDIR="`pwd`"
# Current script directory
cd "`dirname "$0"`" ;
SCRIPTS_DIR="`pwd`" ;
# Current python version
CURRENT_PYTHON_VERSION=$(python3 --version | awk '{print $2}')
# Preserve only mayor and intermediate version numbers from CURRENT_PYTHON_VERSION
CURRENT_PYTHON_VERSION=$(echo "${CURRENT_PYTHON_VERSION}" | awk -F. '{print $1"."$2}')
# Application directory
APP_NAME="get_pokemon_data"

cd "${SCRIPTS_DIR}/${APP_NAME}"
python3 -m venv venv
source venv/bin/activate
if [ -f "requirements.txt" ]; then
    if [ ! -d "venv/lib/python${CURRENT_PYTHON_VERSION}/site-packages" ]; then
        pip install -r requirements.txt
    fi
else
    # pip install minio
    # pip freeze > requirements.txt
fi
python3 -m ${APP_NAME}
deactivate
# rm -rf venv
