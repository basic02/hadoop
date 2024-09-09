#!/bin/bash
set -eo pipefail

SCRIPT_PATH=$(basename $0)
SCRIPT_NAME=${SCRIPT_PATH##*/}
printlog(){
  echo "$SCRIPT_NAME: $1"
}

HERE=$(basename "$PWD")
if [[ "$HERE" != "hdfs-rbf-parcel" ]]; then
  printlog "ERROR: Please only execute in the hdfs-rbf-parcel/ directory";
  exit 1;
fi

PROJECT_VERSION=$1
BUILD_DIRECTORY=$2
TAR_GZ_FILENAME=$3
PARCEL_BASENAME=$4

printlog "INFO: Creating symbolic link of /etc/hadoop/conf under lib/hdfs_rbf"
cd "${BUILD_DIRECTORY}/${PARCEL_BASENAME}/lib/hdfs_rbf"
ln -sf "/etc/hadoop/conf" "conf"

printlog "INFO: Creating symbolic links under lib/native"
cd "${BUILD_DIRECTORY}/${PARCEL_BASENAME}/lib/hdfs_rbf/lib/native"
if [[ -f "libhdfs.so.0.0.0" ]]; then
  ln -sf "libhdfs.so.0.0.0" "libhdfs.so"
fi
if [[ -f "libhdfspp.so.0.1.0" ]]; then
  ln -sf "libhdfspp.so.0.1.0" "libhdfspp.so"
fi

printlog "INFO: Changing to ${BUILD_DIRECTORY} and creating ${TAR_GZ_FILENAME} from ${PARCEL_BASENAME}"
cd "${BUILD_DIRECTORY}"
tar -czf "${TAR_GZ_FILENAME}" -C "${BUILD_DIRECTORY}" "${PARCEL_BASENAME}"
