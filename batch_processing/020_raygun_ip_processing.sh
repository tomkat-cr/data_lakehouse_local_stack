#!/bin/bash
# 020_raygun_ip_processing.sh
# 2024-06-24 | CR
# Run raygun_ip_processing.py to process the raygun error data
#
# IMPORTANT: this most be run in the spark docker container.
#
# docker exec -it spark bash
# cd Project
# sh ./batch_processing/020_raygun_ip_processing.sh
#
set -e
REPO_BASEDIR="`pwd`"
cd "`dirname "$0"`" ;
SCRIPTS_DIR="`pwd`" ;
cd "${SCRIPTS_DIR}/raygun_ip_processing"
python3 -m venv venv
source venv/bin/activate
if [ -f "requirements.txt" ]; then
    pip install -r requirements.txt
else
    pip install pyspark \
        s3fs \
        minio \
        pyhive \
        trino \
        python-dotenv
    pip freeze > requirements.txt
fi
python3 -m raygun_ip_processing
deactivate
rm -rf venv
