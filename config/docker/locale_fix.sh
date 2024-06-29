#/bin/bash
# locale_fix.sh
apt-get -y clean && apt-get update
apt-get -y install locales
locale-gen en_US.UTF-8
