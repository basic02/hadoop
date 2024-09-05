#!/bin/bash

# Set extra variable to let underlying script known
case $1 in
  (start_router)
    export HDFS_RBF_ROLE_TYPE="router"
    ;;
  (*)
    echo "Don't understand [$1]"
    exit 1
    ;;
esac

. $(cd $(dirname $0) && pwd)/common.sh

echo "Running HDFS RBF command: $1"
case $1 in
  (start_router)
    start_router
    ;;
  (*)
    echo "Don't understand [$1]"
    exit 1
    ;;
esac
