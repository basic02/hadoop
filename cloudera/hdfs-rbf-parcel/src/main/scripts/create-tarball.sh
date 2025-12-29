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
if ls libhadoop.so.* > /dev/null 2>&1; then
  ln -sf libhadoop.so.* libhadoop.so
fi
if ls libhdfspp.so.* > /dev/null 2>&1; then
  ln -sf libhdfspp.so.* libhdfspp.so
fi
if ls libhdfs.so.* > /dev/null 2>&1; then
  ln -sf libhdfs.so.* libhdfs.so
fi
if ls libisal.so.* > /dev/null 2>&1; then
  ln -sf libisal.so.* libisal.so
fi
if ls libnativetask.so.* > /dev/null 2>&1; then
  ln -sf libnativetask.so.* libnativetask.so
fi
if ls libsnappy.so.* > /dev/null 2>&1; then
  ln -sf libsnappy.so.* libsnappy.so
fi
if ls libzstd.so.* > /dev/null 2>&1; then
  ln -sf libzstd.so.* libzstd.so
fi

printlog "INFO: Changing to ${BUILD_DIRECTORY} and creating ${TAR_GZ_FILENAME} from ${PARCEL_BASENAME}"
cd "${BUILD_DIRECTORY}"
tar -czf "${TAR_GZ_FILENAME}" -C "${BUILD_DIRECTORY}" "${PARCEL_BASENAME}"
