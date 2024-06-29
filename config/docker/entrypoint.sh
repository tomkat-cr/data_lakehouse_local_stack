#bin/bash
export JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:/bin/java::");
cd "`dirname "$0"`" ;
SCRIPTS_DIR="`pwd`" ;
sh locale_fix.sh
/home/LocalLakeHouse/venv/bin/jupyter lab --allow-root --no-browser --ip=0.0.0.0
