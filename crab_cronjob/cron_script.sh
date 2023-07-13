#!/bin/bash

set -e
MY_PATH="$(dirname -- "${BASH_SOURCE[0]}")"
cd "$MY_PATH"
source /cvmfs/sft.cern.ch/lcg/views/LCG_103swan/x86_64-centos7-gcc11-opt/setup.sh
source /cvmfs/sft.cern.ch/lcg/etc/hadoop-confext/hadoop-swan-setconf.sh analytix 3.2 spark3
spark-submit --master yarn --packages org.apache.spark:spark-avro_2.12:3.3.1 $1
