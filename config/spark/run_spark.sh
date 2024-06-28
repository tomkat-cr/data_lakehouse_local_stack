#/bin/bash
# run_spark.sh
if [ ! -f /opt/spark/conf/spark-defaults.conf.bak ]; then
    cp /opt/spark/conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf.bak
fi
cd "`dirname "$0"`" ;
SCRIPTS_DIR="`pwd`" ;
cp ./spark-defaults.conf /opt/spark/conf/spark-defaults.conf
pyspark
