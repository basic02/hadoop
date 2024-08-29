#!/bin/bash

#
# Set of utility functions shared across HDFS RBF CSDs.
#
################################# misc #################################

function replace {
  # Add this line to support @ character
  replaceText=$(echo ${2}|sed 's/@/\\@/g')
  perl -pi -e "s#${1}#${replaceText}#g" $3
}

function change_xml_value {
  name=$1
  value=$2
  file=$3
  sed -i "/>${name}</,/property/ s#<value>.*</value>#<value>${value}</value>#g" ${file} || true
}

function log {
  timestamp=$(date)
  echo "$timestamp: $1"       #stdout
  echo "$timestamp: $1" 1>&2; #stderr
}

function generate_configuration_files {
  replace "\{\{ZOOKEEPER_QUORUM}}" "${ZK_QUORUM}" ${HDFS_RBF_SITE}

  if [[ $HDFS_RBF_ROLE_TYPE == "router" ]]; then
    if [[ "${ROUTER_SECRET_MANAGER_CLASS}" == "org.apache.hadoop.hdfs.server.federation.router.security.token.SQLDelegationTokenSecretManagerImpl" ]]; then
      if [[ "${SECRET_MANAGER_DATABASE_TYPE}" == "mysql" ]]; then
        $(${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/add_update_property.py ${HDFS_RBF_SITE} "sql-dt-secret-manager.connection.driver" "com.mysql.jdbc.Driver")
      elif [[ "${SECRET_MANAGER_DATABASE_TYPE}" == "postgresql" ]]; then
        $(${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/add_update_property.py ${HDFS_RBF_SITE} "sql-dt-secret-manager.connection.driver" "org.postgresql.Driver")
      elif [[ "${SECRET_MANAGER_DATABASE_TYPE}" == "oracle" ]]; then
        $(${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/add_update_property.py ${HDFS_RBF_SITE} "sql-dt-secret-manager.connection.driver" "oracle.jdbc.driver.OracleDriver")
      fi

      if [ -f ${CONF_DIR}/creds.localjceks ]; then
        rm -f ${CONF_DIR}/creds.localjceks
      fi
      if [[ "${GENERATE_JCEKS_PASSWORD}" == "true" ]]; then
        export HADOOP_CREDSTORE_PASSWORD=${SECRET_MANAGER_CONNECTION_PASSWORD}
      fi
      hadoop credential create sql-dt-secret-manager.connection.password -value ${SECRET_MANAGER_CONNECTION_PASSWORD} -provider localjceks://file/${CONF_DIR}/creds.localjceks
      replace "\{\{CMF_CONF_DIR}}" "${CONF_DIR}" ${HDFS_RBF_SITE}
      change_xml_value "sql-dt-secret-manager.connection.password" "********" ${HDFS_RBF_SITE}
    fi

    cp -f ${CONF_DIR}/hadoop-conf/core-site.xml ${CONF_DIR}/
    cp -f ${CONF_DIR}/hadoop-conf/hdfs-site.xml ${CONF_DIR}/
  fi
}

function generate_hadoop_router_opts {
  HEAP_DUMP_OPTS="-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/hdfs_rbf_${USER}-$(echo ${HDFS_RBF_ROLE_TYPE} | tr '[:lower:]' '[:upper:]')_pid{{PID}}.hprof -XX:OnOutOfMemoryError=/opt/cloudera/cm-agent/service/common/killparent.sh"
  export HADOOP_ROUTER_OPTS="${HADOOP_ROUTER_OPTS} -Xms${ROUTER_JAVA_HEAPSIZE}m -Xmx${ROUTER_JAVA_HEAPSIZE}m ${ROUTER_JAVA_EXTRA_OPTS} ${HEAP_DUMP_OPTS}"
  echo "Generated HADOOP_ROUTER_OPTS: ${HADOOP_ROUTER_OPTS}"
}

################################# service commands #################################

function start_router {
  generate_hadoop_router_opts

  echo "Start Router command: hdfs.sh [\"dfsrouter\"]"
  exec  $(cd $(dirname $0) && pwd)/hdfs.sh --daemon start dfsrouter
}

function create_sql_token_store_tables {
}

function upgrade_sql_token_store_tables {
}

####################################################################################

set -ex

echo "Running HDFS RBF CSD control script..."
echo "Detected CDH_VERSION of [$CDH_VERSION]"
echo "Role type: ${HDFS_RBF_ROLE_TYPE}"

# Set this to not source defaults
export BIGTOP_DEFAULTS_DIR=""

# If HADOOP_HOME is not set, make it the default
DEFAULT_HADOOP_HOME=/opt/cloudera/parcels/HDFS_RBF/lib/hdfs_rbf
HADOOP_HOME=${CDH_HADOOP_HOME:-$DEFAULT_HADOOP_HOME}
export HADOOP_HOME=$(readlink -m "${HADOOP_HOME}")
export HADOOP_HDFS_HOME=$HADOOP_HOME
export HADOOP_LIBEXEC_DIR=$HADOOP_HOME/libexec
export HADOOP_LOG_DIR=${HADOOP_LOG_DIR:-/var/log/hdfs-rbf}
export HADOOP_CLASSPATH=/etc/hadoop/conf:$HADOOP_HOME:$HADOOP_HOME/lib/*:$HADOOP_HOME/*

# HDFS RBF site xml file
export HDFS_RBF_SITE="${CONF_DIR}/hdfs-rbf-site.xml"

# Make sure PARCELS_ROOT is in the format we expect, canonicalized and without a trailing slash.
export PARCELS_ROOT=$(readlink -m "$PARCELS_ROOT")

PYTHON_COMMAND_DEFAULT_INVOKER=/opt/cloudera/cm-agent/bin/python
PYTHON_COMMAND_INVOKER=${PYTHON_COMMAND_INVOKER:-$PYTHON_COMMAND_DEFAULT_INVOKER}

generate_configuration_files
