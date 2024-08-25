#!/bin/bash

#
# Set of utility functions shared across Hive CSDs.
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

# Check config directory
function check_conf_dir {
  # Check status for current config directory
  if [ ! -d "${HIVE_CONF_DIR}" ]; then
    echo "${HIVE_CONF_DIR} doesn't exist, please run client deployment first"
    return 1
  fi

  # Check each config files
  conf_files="hive-site.xml hive-env.sh"
  for conf in $conf_files; do
    if [ ! -f "${HIVE_CONF_DIR}/$conf" ]; then
      echo "${HIVE_CONF_DIR}/$conf doesn't exist, please run client deployment first"
      return 1
    fi
  done

  return 0
}

function update_metastore_uris {
  METASTORE_URIS=$(${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/get_property.py "hive.metastore.uris" ${HIVE_SITE})
  if [[ -n "${METASTORE_URIS}" && "${METASTORE_URIS}" == "{{HIVE_METASTORE_URIS}}" ]]; then
    PROPERTY_FILE=${HIVE_CONF_DIR}/metastores.properties
    if [ -f $PROPERTY_FILE ]; then
      METASTORE_URIS=""
      for host in $(cat ${PROPERTY_FILE} | awk -F: '{print $1}'); do
        METASTORE_URIS="${METASTORE_URIS} thrift://${host}:${HIVE_METASTORE_PORT}"
      done;
      METASTORE_URIS=$(echo "${METASTORE_URIS}" | sed -e 's/^\s*//' | sed 's/ /,/g')
      replace "\{\{HIVE_METASTORE_URIS}}" "${METASTORE_URIS}" ${HIVE_SITE}
    else
      echo "The file metastores.properties doesn't exist. Ignore"
    fi
  fi
}

function update_hive_authentication {
  if [ "${HIVE_SERVER2_ENABLE_LDAP_AUTH}" == "true" ]; then
    replace "\{\{HIVESERVER2_AUTHENTICATION}}" "LDAP" ${HIVE_SITE}
  elif [ "${KERBEROS_AUTH_ENABLED}" == "true" ]; then
    replace "\{\{HIVESERVER2_AUTHENTICATION}}" "KERBEROS" ${HIVE_SITE}
  else
    replace "\{\{HIVESERVER2_AUTHENTICATION}}" "NONE" ${HIVE_SITE}
  fi

  if [ "${HIVE_METASTORE_ENABLE_LDAP_AUTH}" == "true" ]; then
    replace "\{\{HIVE_METASTORE_AUTHENTICATION}}" "LDAP" ${HIVE_SITE}
  elif [ "${KERBEROS_AUTH_ENABLED}" == "true" ]; then
    replace "\{\{HIVE_METASTORE_AUTHENTICATION}}" "KERBEROS" ${HIVE_SITE}
  else
    replace "\{\{HIVE_METASTORE_AUTHENTICATION}}" "NOSASL" ${HIVE_SITE}
  fi
}

function setup_ranger {
  if [[ -n "${RANGER_SERVICE}" && "${RANGER_SERVICE}" != "none" ]]; then
    replace "\{\{HIVE4_KEYTAB}}" "${CONF_DIR}/hive4.keytab" ${CONF_DIR}/ranger-hive-audit.xml

    if [[ "${RANGER_HIVE_HDFS_AUDIT_DIR}" == *"{ranger_base_audit_url}"* ]]; then
      RANGER_HIVE_HDFS_AUDIT_PATH=$(echo ${RANGER_HIVE_HDFS_AUDIT_DIR} | sed -e "s/\${ranger_base_audit_url}//g")
      RANGER_HIVE_HDFS_AUDIT_PATH="${RANGER_AUDIT_BASE_PATH}${RANGER_HIVE_HDFS_AUDIT_PATH}"
    else
      RANGER_HIVE_HDFS_AUDIT_PATH=${RANGER_HIVE_HDFS_AUDIT_DIR}
    fi
    change_xml_value "xasecure.audit.destination.hdfs.dir" "${RANGER_HIVE_HDFS_AUDIT_PATH}" ${CONF_DIR}/ranger-hive-audit.xml

    AUTHORIZATION_MANAGER=$(${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/get_property.py "hive.security.authorization.manager" ${HIVE_SITE})
    if [[ -z "${AUTHORIZATION_MANAGER}" || "${AUTHORIZATION_MANAGER}" != "org.apache.ranger.authorization.hive.authorizer.RangerHiveAuthorizerFactory" ]]; then
      change_xml_value "hive.security.authorization.manager" "org.apache.ranger.authorization.hive.authorizer.RangerHiveAuthorizerFactory" ${HIVE_SITE}
    fi
  fi
}

function setup_atlas {
  if [[ -n "${ATLAS_SERVICE}" && "${ATLAS_SERVICE}" != "none" ]]; then
    replace "\{\{HIVE4_KEYTAB}}" "${CONF_DIR}/hive4.keytab" ${CONF_DIR}/atlas-application.properties

    METASTORE_EVENT_LISTENERS=$(${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/get_property.py "hive.metastore.event.listeners" ${HIVE_SITE})
    if [[ -n "${METASTORE_EVENT_LISTENERS}" && "${METASTORE_EVENT_LISTENERS}" != "None" ]]; then
      if [[ "${METASTORE_EVENT_LISTENERS}" != *"org.apache.atlas.hive.hook.HiveMetastoreHook"* ]]; then
        METASTORE_EVENT_LISTENERS="${METASTORE_EVENT_LISTENERS},org.apache.atlas.hive.hook.HiveMetastoreHook"
      fi
    else
      METASTORE_EVENT_LISTENERS="org.apache.atlas.hive.hook.HiveMetastoreHook"
    fi
    change_xml_value "hive.metastore.event.listeners" ${METASTORE_EVENT_LISTENERS} ${HIVE_SITE}

    HIVE_EXEC_POST_HOOKS=$(${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/get_property.py "hive.exec.post.hooks" ${HIVE_SITE})
    if [[ -n "${HIVE_EXEC_POST_HOOKS}" && "${HIVE_EXEC_POST_HOOKS}" != "None" ]]; then
      if [[ "${HIVE_EXEC_POST_HOOKS}" != *"org.apache.atlas.hive.hook.HiveHook"* ]]; then
        HIVE_EXEC_POST_HOOKS="${HIVE_EXEC_POST_HOOKS},org.apache.atlas.hive.hook.HiveHook"
      fi
    else
      HIVE_EXEC_POST_HOOKS="org.apache.atlas.hive.hook.HiveHook"
    fi
    change_xml_value "hive.exec.post.hooks" ${HIVE_EXEC_POST_HOOKS} ${HIVE_SITE}
  fi
}

function update_hs2_kerberos_principal {
  if [[ -n "${HIVESERVER2_LOAD_BALANCER_HOST}" && "${KERBEROS_AUTH_ENABLED}" == "true" ]]; then
    KERBEROS_PRINCIPAL=$(${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/get_property.py "hive.server2.authentication.kerberos.principal" ${HIVE_SITE})
    KERBEROS_PRIMARY=$(echo $KERBEROS_PRINCIPAL | cut -d "/" -f 1)
    KERBEROS_REALM=$(echo $KERBEROS_PRINCIPAL | cut -d "/" -f 2 | cut -d "@" -f 2)
    NEW_KERBEROS_PRINCIPAL="${KERBEROS_PRIMARY}/${HIVESERVER2_LOAD_BALANCER_HOST}@${KERBEROS_REALM}"
    change_xml_value "hive.server2.authentication.kerberos.principal" "${NEW_KERBEROS_PRINCIPAL}" ${HIVE_SITE}
  fi
}

function generate_configuration_files {
  if [[ -z "${HIVE_JDBC_URL_OVERRIDE}" ]]; then
    if [[ "${HIVE_METASTORE_DATABASE_SSL_ENABLED}" == "true" ]]; then
      HIVE_JDBC_URL_OVERRIDE="jdbc:${HIVE_METASTORE_DATABASE_TYPE}://${HIVE_METASTORE_DATABASE_HOST}:${HIVE_METASTORE_DATABASE_PORT}/${HIVE_METASTORE_DATABASE_NAME}?useUnicode=true\&amp;characterEncoding=UTF-8\&amp;useSSL=true"
    else
      HIVE_JDBC_URL_OVERRIDE="jdbc:${HIVE_METASTORE_DATABASE_TYPE}://${HIVE_METASTORE_DATABASE_HOST}:${HIVE_METASTORE_DATABASE_PORT}/${HIVE_METASTORE_DATABASE_NAME}?useUnicode=true\&amp;characterEncoding=UTF-8\&amp;useSSL=false"
    fi
    change_xml_value "javax.jdo.option.ConnectionURL" ${HIVE_JDBC_URL_OVERRIDE} ${HIVE_SITE}
  fi

  if [[ "${HIVE_METASTORE_DATABASE_TYPE}" == "mysql" ]]; then
    replace "\{\{HIVE_METASTORE_CONNECTION_DRIVER_NAME}}" "com.mysql.jdbc.Driver" ${HIVE_SITE}
  elif [[ "${HIVE_METASTORE_DATABASE_TYPE}" == "postgresql" ]]; then
    replace "\{\{HIVE_METASTORE_CONNECTION_DRIVER_NAME}}" "org.postgresql.Driver" ${HIVE_SITE}
  elif [[ "${HIVE_METASTORE_DATABASE_TYPE}" == "oracle" ]]; then
    replace "\{\{HIVE_METASTORE_CONNECTION_DRIVER_NAME}}" "oracle.jdbc.driver.OracleDriver" ${HIVE_SITE}
  fi

  replace "\{\{ZOOKEEPER_QUORUM}}" "${ZK_QUORUM}" ${HIVE_SITE}
  ZK_PORT=${ZK_QUORUM##*:}
  replace "\{\{ZOOKEEPER_CLIENT_PORT}}" "${ZK_PORT}" ${HIVE_SITE}

  if [[ $HIVE_ROLE_TYPE == "hivemetastore" || $HIVE_ROLE_TYPE == "hiveserver2" ]]; then
    if [ -f ${CONF_DIR}/creds.localjceks ]; then
      rm -f ${CONF_DIR}/creds.localjceks
    fi
    if [[ "${GENERATE_JCEKS_PASSWORD}" == "true" ]]; then
      export HADOOP_CREDSTORE_PASSWORD=${HIVE_METASTORE_DATABASE_PASSWORD}
    fi
    hadoop credential create javax.jdo.option.ConnectionPassword -value ${HIVE_METASTORE_DATABASE_PASSWORD} -provider localjceks://file/${CONF_DIR}/creds.localjceks
    replace "\{\{CMF_CONF_DIR}}" "${CONF_DIR}" ${HIVE_SITE}
    change_xml_value "javax.jdo.option.ConnectionPassword" "********" ${HIVE_SITE}

    cp -f ${CONF_DIR}/hadoop-conf/core-site.xml ${HIVE_CONF_DIR}/
    cp -f ${CONF_DIR}/hadoop-conf/hdfs-site.xml ${HIVE_CONF_DIR}/
  fi

  if [[ $HIVE_ROLE_TYPE == "hiveserver2" ]]; then
    AUX_JARS=$(${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/get_property.py "hive.aux.jars.path" ${HIVE_SITE})
    if [[ -n "${AUX_JARS}" && "${AUX_JARS}" == "{{DEFAULT_AUX_JARS_PATH}}" ]]; then
      DEFAULT_AUX_JARS="file://${HIVE_HOME}/lib/hive-hbase-handler.jar,file://${HADOOP_HOME}/../hbase/hbase-client.jar,file://${HADOOP_HOME}/../hbase/hbase-protocol.jar,file://${HADOOP_HOME}/../hbase/hbase-hadoop2-compat.jar,file://${HADOOP_HOME}/../hbase/hbase-server.jar,file://${HADOOP_HOME}/../hbase/hbase-common.jar,file://${HADOOP_HOME}/../hbase/hbase-hadoop-compat.jar"
      replace "\{\{DEFAULT_AUX_JARS_PATH}}" "${DEFAULT_AUX_JARS}" ${HIVE_SITE}
    fi
  fi

  if [[ $HIVE_ROLE_TYPE == "hivemetastore" ]]; then
    TTL=$(${PYTHON_COMMAND_INVOKER} ${CONF_DIR}/scripts/get_property.py "hive.metastore.event.db.listener.timetolive" ${HIVE_SITE})
    if [[ -n "${TTL}" ]]; then
      TTL="${TTL}s"
      change_xml_value "hive.metastore.event.db.listener.timetolive" ${TTL} ${HIVE_SITE}
    fi
  fi

  if [[ $HIVE_ROLE_TYPE == "client" ]]; then
    replace "\{\{ZOOKEEPER_QUORUM}}" "${ZK_QUORUM}" ${HIVE_CONF_DIR}/beeline-site.xml

    cp -f ${CONF_DIR}/hadoop-conf/core-site.xml ${HIVE_CONF_DIR}/
    cp -f ${CONF_DIR}/hadoop-conf/hdfs-site.xml ${HIVE_CONF_DIR}/
    if [ -f ${CONF_DIR}/hadoop-conf/ozone-site.xml ]; then
      cp -f ${CONF_DIR}/hadoop-conf/ozone-site.xml ${HIVE_CONF_DIR}/
    fi
    cp -f ${CONF_DIR}/yarn-conf/mapred-site.xml ${HIVE_CONF_DIR}/
    cp -f ${CONF_DIR}/yarn-conf/yarn-site.xml ${HIVE_CONF_DIR}/
  fi

  update_metastore_uris
  update_hive_authentication
  setup_ranger
  setup_atlas
  update_hs2_kerberos_principal
}

function generate_hadoop_client_opts {
  HEAP_DUMP_OPTS="-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/hive4_${USER}-$(echo ${HIVE_ROLE_TYPE} | tr '[:lower:]' '[:upper:]')_pid{{PID}}.hprof -XX:OnOutOfMemoryError=/opt/cloudera/cm-agent/service/common/killparent.sh"
  export HADOOP_CLIENT_OPTS="${HADOOP_CLIENT_OPTS} -Xms${HIVE_JAVA_HEAPSIZE}m -Xmx${HIVE_JAVA_HEAPSIZE}m ${HIVE_JAVA_EXTRA_OPTS} ${HEAP_DUMP_OPTS}"
  echo "Generated HADOOP_CLIENT_OPTS: ${HADOOP_CLIENT_OPTS}"
}

function populate_hive_env_values() {
  HIVE_AUX_JARS_PATH_STR=${HIVE_AUX_JARS_PATH}
  if [[ -n "${HIVE_AUX_JARS_PATH_STR}" ]]; then
    HIVE_AUX_JARS_PATH_STR="\$([[ -d ${HIVE_AUX_JARS_PATH_STR} ]] \&\& sed \"s: :,:g\" <<< \$(find ${HIVE_AUX_JARS_PATH_STR} -name \"*.jar\" 2> /dev/null)),"
  fi
  sed -i "s#{{HIVE_AUX_JARS_PATH}}#${HIVE_AUX_JARS_PATH_STR}#g" ${1} || true
  echo "export HADOOP_CLIENT_OPTS=\"-Xmx${HIVE_CLIENT_JAVA_HEAPSIZE}m ${HIVE_CLIENT_JAVA_OPTS} \$HADOOP_CLIENT_OPTS\"" >> ${1}
}

################################# service commands #################################

function start_metastore {
  generate_hadoop_client_opts

  echo "Start Metastore command: hive.sh [\"metastore\", \"-p\", \"${HIVE_METASTORE_PORT}\"]"
  exec $(cd $(dirname $0) && pwd)/hive.sh metastore -p ${HIVE_METASTORE_PORT}
}

function start_hiveserver2 {
  generate_hadoop_client_opts

  echo "Start HiveServer2 command: hive.sh [\"hiveserver2\"]"
  exec  $(cd $(dirname $0) && pwd)/hive.sh hiveserver2
}

function create_metastore_tables {
  echo "Create Metastore Tables command: hive.sh [\"create_metastore_tables\"]"
  exec $(cd $(dirname $0) && pwd)/hive.sh create_metastore_tables
}

function validate_metastore {
  echo "Validate Metastore command: hive.sh [\"validate_metastore\"]"
  exec $(cd $(dirname $0) && pwd)/hive.sh validate_metastore
}

function upgrade_metastore {
  echo "Upgrade Metastore command: hive.sh [\"upgrade_metastore\"]"
  exec $(cd $(dirname $0) && pwd)/hive.sh upgrade_metastore
}

function deploy_client_config {
  populate_hive_env_values ${HIVE_CONF_DIR}/hive-env.sh
  echo "Deploy done"
}

################################# service commands #################################

set -ex

echo "Running HIVE4 CSD control script..."
echo "Detected CDH_VERSION of [$CDH_VERSION]"
echo "Role type: ${HIVE_ROLE_TYPE}"

# Set this to not source defaults
export BIGTOP_DEFAULTS_DIR=""

export HADOOP_HOME=${HADOOP_HOME:-$(readlink -m "$CDH_HADOOP_HOME")}

if [[ $HIVE_ROLE_TYPE = "client" ]]; then
  export HIVE_CONF_DIR="${CONF_DIR}/hive4-conf"
else
  export HIVE_CONF_DIR="${CONF_DIR}"
fi
# If HIVE_HOME is not set, make it the default
DEFAULT_HIVE4_HOME=/opt/cloudera/parcels/HIVE4/lib/hive4
HIVE_HOME=${CDH_HIVE4_HOME:-$DEFAULT_HIVE4_HOME}
export HIVE_HOME=$(readlink -m "${HIVE_HOME}")
# Hive site xml file
export HIVE_SITE="${HIVE_CONF_DIR}/hive-site.xml"

# Make sure PARCELS_ROOT is in the format we expect, canonicalized and without a trailing slash.
export PARCELS_ROOT=$(readlink -m "$PARCELS_ROOT")

PYTHON_COMMAND_DEFAULT_INVOKER=/opt/cloudera/cm-agent/bin/python
PYTHON_COMMAND_INVOKER=${PYTHON_COMMAND_INVOKER:-$PYTHON_COMMAND_DEFAULT_INVOKER}

generate_configuration_files
