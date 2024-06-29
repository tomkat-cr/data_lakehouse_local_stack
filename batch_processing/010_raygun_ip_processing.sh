#!/bin/bash
# 010_raygun_ip_processing.sh
# 2024-06-24 | CR
# Run raygun_ip_processing.py to process the raygun error data
#
# IMPORTANT: this most be run in the spark docker container.
#
# docker exec -it spark bash
# cd Project
# bash ./batch_processing/010_raygun_ip_processing.sh
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
# Application entry point
APP_NAME="main"
# Application directory
APP_DIR="raygun_ip_processing"

cd "${SCRIPTS_DIR}/${APP_DIR}"
python3 -m venv venv
source venv/bin/activate
if [ -f "requirements.txt" ]; then
    if [ ! -d "venv/lib/python${CURRENT_PYTHON_VERSION}/site-packages" ]; then
        pip install -r requirements.txt
    fi
else
    pip install pyspark \
        s3fs \
        minio \
        pyhive \
        trino \
        python-dotenv \
        boto3 \
        botocore
    pip freeze > requirements.txt
fi
python3 -m ${APP_NAME} ${MODE}
deactivate
# rm -rf venv
