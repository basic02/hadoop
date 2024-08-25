#!/bin/bash

# Set extra variable to let underlying script known
case $1 in
  (start_metastore)
    export HIVE_ROLE_TYPE="hivemetastore"
    ;;
  (start_hiveserver2)
    export HIVE_ROLE_TYPE="hiveserver2"
    ;;
  (create_metastore_tables)
    export HIVE_ROLE_TYPE="command"
    ;;
  (validate_metastore)
    export HIVE_ROLE_TYPE="command"
    ;;
  (upgrade_metastore)
    export HIVE_ROLE_TYPE="command"
    ;;
  (client)
    export HIVE_ROLE_TYPE="client"
    ;;
  (*)
    echo "Don't understand [$1]"
    exit 1
    ;;
esac

. $(cd $(dirname $0) && pwd)/common.sh

echo "Running HIVE4 command: $1"
case $1 in
  (start_metastore)
    start_metastore
    ;;
  (start_hiveserver2)
    start_hiveserver2
    ;;
  (create_metastore_tables)
    create_metastore_tables
    ;;
  (validate_metastore)
    validate_metastore
    ;;
  (upgrade_metastore)
    upgrade_metastore
    ;;
  (client)
    deploy_client_config
    ;;
  (*)
    echo "Don't understand [$1]"
    exit 1
    ;;
esac
