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
  replace "\{\{CMF_CONF_DIR}}" "${CONF_DIR}" ${HDFS_RBF_SITE}

  ROUTER_RPC_ADDRESS=$(${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/get_property.py "dfs.federation.router.rpc-address" ${HDFS_RBF_SITE})
  if [[ -z "${ROUTER_RPC_ADDRESS}" || "${ROUTER_RPC_ADDRESS}" == "None" ]]; then
    change_xml_value "dfs.federation.router.rpc-address" "${HOST}:${ROUTER_RPC_PORT}" ${HDFS_RBF_SITE}
  fi
  ROUTER_ADMIN_ADDRESS=$(${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/get_property.py "dfs.federation.router.admin-address" ${HDFS_RBF_SITE})
  if [[ -z "${ROUTER_ADMIN_ADDRESS}" || "${ROUTER_ADMIN_ADDRESS}" == "None" ]]; then
    change_xml_value "dfs.federation.router.admin-address" "${HOST}:${ROUTER_ADMIN_PORT}" ${HDFS_RBF_SITE}
  fi
  ROUTER_HTTP_ADDRESS=$(${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/get_property.py "dfs.federation.router.http-address" ${HDFS_RBF_SITE})
  if [[ -z "${ROUTER_HTTP_ADDRESS}" || "${ROUTER_HTTP_ADDRESS}" == "None" ]]; then
    change_xml_value "dfs.federation.router.http-address" "${HOST}:${ROUTER_HTTP_PORT}" ${HDFS_RBF_SITE}
  fi
  ROUTER_HTTPS_ADDRESS=$(${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/get_property.py "dfs.federation.router.https-address" ${HDFS_RBF_SITE})
  if [[ -z "${ROUTER_HTTPS_ADDRESS}" || "${ROUTER_HTTPS_ADDRESS}" == "None" ]]; then
    change_xml_value "dfs.federation.router.https-address" "${HOST}:${ROUTER_HTTPS_PORT}" ${HDFS_RBF_SITE}
  fi

  if [[ $HDFS_RBF_ROLE_TYPE == "router" ]]; then
    if [[ "${ROUTER_SECRET_MANAGER_CLASS}" == "org.apache.hadoop.hdfs.server.federation.router.security.token.SQLDelegationTokenSecretManagerImpl" ]]; then
      CONNECTION_DRIVER=$(${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/get_property.py "sql-dt-secret-manager.connection.driver" ${HDFS_RBF_SITE})
      if [[ -z "${CONNECTION_DRIVER}" || "${CONNECTION_DRIVER}" == "None" ]]; then
        if [[ "${SECRET_MANAGER_DATABASE_TYPE}" == "mysql" ]]; then
          CONNECTION_DRIVER="com.mysql.jdbc.Driver"
        elif [[ "${SECRET_MANAGER_DATABASE_TYPE}" == "postgresql" ]]; then
          CONNECTION_DRIVER="org.postgresql.Driver"
        elif [[ "${SECRET_MANAGER_DATABASE_TYPE}" == "oracle" ]]; then
          CONNECTION_DRIVER="oracle.jdbc.driver.OracleDriver"
        fi
        change_xml_value "sql-dt-secret-manager.connection.driver" "${CONNECTION_DRIVER}" ${HDFS_RBF_SITE}
      fi
    fi

    KERBEROS_PRINCIPAL=$(${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/get_property.py "dfs.federation.router.kerberos.principal" ${HDFS_RBF_SITE})
    if [[ -n "${KERBEROS_PRINCIPAL}" ]]; then
      KERBEROS_PRIMARY=$(echo $KERBEROS_PRINCIPAL | cut -d "/" -f 1)
      KERBEROS_REALM=$(echo $KERBEROS_PRINCIPAL | cut -d "/" -f 2 | cut -d "@" -f 2)
      export SCM_KERBEROS_PRINCIPAL="${KERBEROS_PRIMARY}/${HOST}@${KERBEROS_REALM}"
    fi

    cp -f ${CONF_DIR}/hadoop-conf/core-site.xml ${CONF_DIR}/
    cp -f ${CONF_DIR}/hadoop-conf/hdfs-site.xml ${CONF_DIR}/
  fi
}

function generate_hadoop_router_opts {
  export HADOOP_ROUTER_OPTS="${HADOOP_ROUTER_OPTS} -Xms${ROUTER_JAVA_HEAPSIZE}m -Xmx${ROUTER_JAVA_HEAPSIZE}m ${ROUTER_JAVA_EXTRA_OPTS} ${CSD_JAVA_OPTS}"
  echo "Generated HADOOP_ROUTER_OPTS: ${HADOOP_ROUTER_OPTS}"
}

################################# service commands #################################

function start_router {
  generate_hadoop_router_opts

  echo "Start router command: hdfs.sh [\"dfsrouter\"]"
  exec  $(cd $(dirname $0) && pwd)/hdfs.sh dfsrouter
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
DEFAULT_HADOOP_HOME=$(readlink -m "${DEFAULT_HADOOP_HOME}")
# Temporarily set CDH_HADOOP_HOME & CDH_HDFS_HOME environment variable here because hdfs_rbf_env.sh is not executed
# when calling source_parcel_environment
export CDH_HADOOP_HOME=$DEFAULT_HADOOP_HOME
export CDH_HDFS_HOME=$DEFAULT_HADOOP_HOME
HADOOP_HOME=${CDH_HADOOP_HOME:-$DEFAULT_HADOOP_HOME}
export HADOOP_HOME=$(readlink -m "${HADOOP_HOME}")
export HADOOP_HOME_WARN_SUPPRESS=true
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export HADOOP_YARN_HOME=$HADOOP_HOME
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_LIBEXEC_DIR=$HADOOP_HOME/libexec
export JAVA_LIBRARY_PATH=$HADOOP_HOME/lib/native
export HADOOP_CLASSPATH=$HADOOP_HOME:$HADOOP_HOME/lib/*.jar
export HADOOP_LOGFILE=hadoop-cmf-hdfs-ROUTER-${HOST}.log.out

if [[ -n "${DB_CONNECTOR_JAR_DIR}" ]]; then
  export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$DB_CONNECTOR_JAR_DIR/*.jar
fi

# HDFS RBF site xml file
export HDFS_RBF_SITE="${CONF_DIR}/hdfs-rbf-site.xml"

# Make sure PARCELS_ROOT is in the format we expect, canonicalized and without a trailing slash.
export PARCELS_ROOT=$(readlink -m "$PARCELS_ROOT")

PYTHON_COMMAND_DEFAULT_INVOKER=/opt/cloudera/cm-agent/bin/python
PYTHON_COMMAND_INVOKER=${PYTHON_COMMAND_INVOKER:-$PYTHON_COMMAND_DEFAULT_INVOKER}

generate_configuration_files
