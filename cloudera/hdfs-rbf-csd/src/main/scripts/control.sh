#!/bin/bash

# Set extra variable to let underlying script known
case $1 in
  (start_router)
    export HDFS_RBF_ROLE_TYPE="router"
    ;;
  (create_sql_token_store_tables)
    export HDFS_RBF_ROLE_TYPE="command"
    ;;
  (upgrade_sql_token_store_tables)
    export HDFS_RBF_ROLE_TYPE="command"
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
  (create_sql_token_store_tables)
    create_sql_token_store_tables
    ;;
  (upgrade_sql_token_store_tables)
    upgrade_sql_token_store_tables
    ;;
  (*)
    echo "Don't understand [$1]"
    exit 1
    ;;
esac
