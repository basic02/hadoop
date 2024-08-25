#!/bin/bash

HDFS_RBF_DIRNAME=${PARCEL_DIRNAME:-"HDFS_RBF"}
export CDH_HDFS_RBF_HOME=$PARCELS_ROOT/$HDFS_RBF_DIRNAME/lib/hdfs_rbf
export CDH_HDFS_RBF_BIN=$PARCELS_ROOT/$HDFS_RBF_DIRNAME/bin

if [[ -x "$(command -v hadoop)" ]]; then
  export HADOOP_CLASSPATH=$(hadoop classpath)
fi
