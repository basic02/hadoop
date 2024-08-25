#!/bin/bash

BLACKLIST=(
org/apache/hadoop/hadoop-cloudera
org/apache/hadoop/hdfs-rbf-csd
org/apache/hadoop/hdfs-rbf-parcel
)

if [ "$#" -ne 1 ]; then
    echo "Please provide Maven repository base directory as the sole argument."
    exit 1
fi

BASE_FOLDER=$1

for folder in "${BLACKLIST[@]}"
do
  echo "$BASE_FOLDER/$folder"
  rm -rf "${BASE_FOLDER:?}/${folder:?}"
done
