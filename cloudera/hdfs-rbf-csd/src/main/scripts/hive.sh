#!/bin/bash

# Copyright (c) 2013 Cloudera, Inc. All rights reserved.

set -o pipefail
# debug
set -x

# Time marker for both stderr and stdout
date; date 1>&2

cloudera_config=/opt/cloudera/cm-agent/service/common
. ${cloudera_config}/cloudera-config.sh

# Load the parcel environment
source_parcel_environment

# attempt to find java
locate_cdh_java_home

set_hive_classpath

HADOOP_CLIENT_OPTS=$(replace_pid $HADOOP_CLIENT_OPTS)

get_gc_args() {
  # hive can supply JVM GC args here [ applicable for CDH >=6.3.0 ]

  # Uncomment below to override GC arguments provided by cloudera-config.sh:
  ##  JAVA8_GC_TUNING_ARGS="GC argument for JAVA8"
  ##  JAVA11_GC_TUNING_ARGS="GC argument for JAVA8"
  # function from cloudera-config.sh provides BASIC_GC_TUNING_ARGS based on java version
  set_basic_gc_tuning_args_based_on_java_version
}

# HIVE has been tuning hiveserver2, hivemetastore and webhcat roles with the
# same GC parameters.
get_gc_args
HIVE_GC_ARGS="$BASIC_GC_TUNING_ARGS"

get_generic_java_opts
export HADOOP_CLIENT_OPTS="${HADOOP_CLIENT_OPTS} ${GENERIC_JAVA_OPTS}"

# If safety valve ENV value is in-use, then user provided GC args are used
[[ ! -z $HIVE_JVM_GC_ARGS_SAFETY_VALVE ]] && HIVE_GC_ARGS="$HIVE_JVM_GC_ARGS_SAFETY_VALVE"

# Now, replace the final GC args within the respective OPTs args
# NOTE: the below replacement only works if CDH >= 6.3.0 [ i.e the version (& future versions) that supports java 11 for CDH ]
# NOTE: all HIVE roles share a single variable (HADOOP_CLIENT_OPTS) for JVM OPTS:
export HADOOP_CLIENT_OPTS=$(replace_gc_args "$HADOOP_CLIENT_OPTS" "$HIVE_GC_ARGS")

if [ "$CDH_VERSION" -ge "4" ]; then
  # Disable IPv6. This is automatically done by bin/hadoop for CDH3 clients.
  export HADOOP_CLIENT_OPTS="-Djava.net.preferIPv4Stack=true $HADOOP_CLIENT_OPTS"
fi

echo "using $JAVA_HOME as JAVA_HOME"
echo "using $CDH_VERSION as CDH_VERSION"

export HADOOP_HOME=$CDH_HADOOP_HOME
export HIVE_HOME=${HIVE_HOME:-$CDH_HIVE_HOME}
export HBASE_HOME=$CDH_HBASE_HOME
export SENTRY_HOME=$CDH_SENTRY_HOME
export HIVE_CONF_DIR=$CONF_DIR
export HBASE_CONF_DIR=${CONF_DIR}/hbase-conf
export TEZ_CONF_DIR=${CONF_DIR}/tez-conf

if [[ -d $HBASE_CONF_DIR ]]; then
  perl -pi -e "s#\{\{HBASE_CONF_DIR}}#$HBASE_CONF_DIR#g" $HBASE_CONF_DIR/hbase-env.sh
fi

SCHEMATOOL_DBTYPE=$HIVE_METASTORE_DATABASE_TYPE
# schematool uses different dbtype alias for postgres
if [[ "$SCHEMATOOL_DBTYPE" == postgresql ]]; then
  SCHEMATOOL_DBTYPE="postgres"
  # OPSAPS-16280 tell hive schematool to be compatible with postgres 8.1
  POSTGRESQL81_WORKAROUND="-dbOpts postgres.filter.81,postgres.filter.pre.9"
fi

function check_version_run_schematool_upgrade() {
  VERSION=$1
  if [[ $VERSION -ge "" ]]; then
    exec bash -x $HIVE_HOME/bin/schematool -verbose -dbType $SCHEMATOOL_DBTYPE -upgradeSchema $POSTGRESQL81_WORKAROUND
    exit;
  fi

  # Minimum schema version supported for Oracle is 9, otherwise is 5.
  # Schema version -1 implies we couldn't figure out the schema version.
  if ([[ "$HIVE_METASTORE_DATABASE_TYPE" == oracle ]] && [[ "$VERSION" -lt "9" ]]) || [[ "$VERSION" -lt "5" ]] ; then
    echo "ERROR: Upgrade is not supported from current schema version for $HIVE_METASTORE_DATABASE_TYPE database type."
    exit 1
  fi
  if [[ "$VERSION" -ge "12" ]]; then
    # Starting schema version 12, HMS schema has the VERSION table
    exec bash -x $HIVE_HOME/bin/schematool -verbose -dbType $SCHEMATOOL_DBTYPE -upgradeSchema $POSTGRESQL81_WORKAROUND
  else
    VERSION_STR="0.$VERSION.0"
    exec bash -x $HIVE_HOME/bin/schematool -verbose -dbType $SCHEMATOOL_DBTYPE -upgradeSchemaFrom $VERSION_STR $POSTGRESQL81_WORKAROUND
  fi
}

# Add hive-hcatalog-server-extensions and kudu-hive plugin jars in classpath.
set_classpath_for_plugin_jars() {
  if [ -z $1 ]; then
    echo "Must call with the name of variable to assign."
    exit 1
  fi

  # include hcatalog jar if asked by CM - INCLUDE_HIVE_HCATALOG_JAR=true
  if [ "$INCLUDE_HIVE_HCATALOG_JAR" = "true" ]; then
    # Here we find a jar for hive-hcatalog-server-extensions and add it to the given
    # environment variable.
    if [[ -z "$CDH_HCAT_HOME" ]]; then
      echo "CDH_HCAT_HOME must be set."
      exit 1
    fi
    # Add hive-hcatalog-server-extensions plugin jar to the classpath.
    ADD_TO_CP=`find "${CDH_HCAT_HOME}/share/hcatalog/" -maxdepth 1 -name 'hive-hcatalog-server-extensions*.jar' | tr "\n" ":"`
    eval OLD_VALUE=\$$1
    NEW_VALUE="$ADD_TO_CP$OLD_VALUE"
    export $1=${NEW_VALUE/%:/}  # Remove trailing ':' if present.
  fi

  # include kudu jar if asked by CM - INCLUDE_KUDU_HIVE_JAR=true
  if [ "$INCLUDE_KUDU_HIVE_JAR" = "true" ]; then
    # Here we find a jar for kudu-hive and add it to the given
    # environment variable.
    if [[ -z "$CDH_KUDU_HOME" ]]; then
      echo "CDH_KUDU_HOME must be set."
      exit 1
    fi
    # Add kudu-hive plugin jar to the classpath.
    eval OLD_VALUE=\$$1
    if [[ -z "$KUDU_HIVE_JAR_PATH" ]]; then
      ADD_TO_CP=`find "${CDH_KUDU_HOME}/" -maxdepth 1 -name 'kudu-hive*.jar' | tr "\n" ":"`
      NEW_VALUE="$ADD_TO_CP$OLD_VALUE"
    else
      NEW_VALUE="$KUDU_HIVE_JAR_PATH:$OLD_VALUE"
    fi
    export $1=${NEW_VALUE/%:/}  # Remove trailing ':' if present.
  fi
}

# Check yarn-conf first because HDFS could also generate hadoop-conf.
if [ -d $CONF_DIR/yarn-conf ]; then
  export HADOOP_CONF_DIR=$CONF_DIR/yarn-conf
  # (See CDH-10123) Hive doesn't use HADOOP_MAPRED_HOME to figure out
  # whether it should use MR1 or 2.
  # So we need to add its contents to classpath manually.
  # Also exporting it so that if it gets fixed in future, we don't have to do anything.
  export HADOOP_MAPRED_HOME=$CDH_MR2_HOME
elif [ -d $CONF_DIR/hadoop-conf ]; then
  export HADOOP_CONF_DIR=$CONF_DIR/hadoop-conf
else
  # (OPSAPS-51558) HMS_ONLY being set to true means its an HMS only mode
  # and so we proceed without exiting.
  if [ "$HMS_ONLY" = "true" ]; then
      echo "No MR config needed, HMS only mode."
  else
      echo "No config directory found for MR."
      exit 1
  fi
fi

# Add contents of HADOOP_MAPRED_HOME to classpath manually
if [ -d $HADOOP_MAPRED_HOME ]; then
  for i in "$HADOOP_MAPRED_HOME/"*.jar; do
    AUX_CLASSPATH="${AUX_CLASSPATH}:$i"
  done
fi

set_classpath_for_plugin_jars AUX_CLASSPATH

echo "using $HIVE_HOME as HIVE_HOME"
echo "using $HIVE_CONF_DIR as HIVE_CONF_DIR"
echo "using $HADOOP_HOME as HADOOP_HOME"
echo "using $HADOOP_CONF_DIR as HADOOP_CONF_DIR"
echo "using $HBASE_HOME as HBASE_HOME"
echo "using $HBASE_CONF_DIR as HBASE_CONF_DIR"

# Search-replace {{CMF_CONF_DIR}} in files
replace_conf_dir

replace_hive_hbase_jars_template "hive-site.xml"

JDBC_JARS="$CLOUDERA_MYSQL_CONNECTOR_JAR:$CLOUDERA_POSTGRESQL_JDBC_JAR:$CLOUDERA_ORACLE_CONNECTOR_JAR"
if [[ -z "$AUX_CLASSPATH" ]]; then
  export AUX_CLASSPATH="$JDBC_JARS"
else
  export AUX_CLASSPATH="$AUX_CLASSPATH:$JDBC_JARS"
fi

if ([[ -n "$HIVE_AUX_JARS_PATH" ]] && [[ ! -d "$HIVE_AUX_JARS_PATH" ]]); then
  $( cat <<EOF
ERROR: HIVE_AUX_JARS_PATH is configured in Cloudera Manager as $HIVE_AUX_JARS_PATH.
However directory $HIVE_AUX_JARS_PATH does not exist. When configured, directory specified
in HIVE_AUX_JARS_PATH must be created and managed manually before starting Hive.
EOF
)
  exit 1
fi

if [[ "$1" == webhcat ]]; then
  export HCAT_PREFIX="$CDH_HCAT_HOME"
  export WEBHCAT_CONF_DIR="$CONF_DIR"
  export HADOOP_PREFIX="$HADOOP_HOME"
  export HADOOP_LIBEXEC_DIR="$CDH_HADOOP_HOME/libexec/"
  export WEBHCAT_LOG_DIR="$HIVE_LOG_DIR"
  export WEBHCAT_LOG4J="file:$WEBHCAT_CONF_DIR/webhcat-log4j.properties"
  echo "using $HCAT_PREFIX as HCAT_PREFIX"
  echo "using $WEBHCAT_CONF_DIR as WEBHCAT_CONF_DIR"
  echo "using $HADOOP_PREFIX as HADOOP_PREFIX"
  echo "using $JAVA_HOME as JAVA_HOME"
  echo "using $HADOOP_LIBEXEC_DIR as HADOOP_LIBEXEC_DIR"
  echo "using $WEBHCAT_LOG_DIR as WEBHCAT_LOG_DIR"
  echo "using $WEBHCAT_LOG4J as WEBHCAT_LOG4J"
  WEBHCAT_SERVER_CMD="$HCAT_PREFIX/sbin/webhcat_server.sh --config \"$WEBHCAT_CONF_DIR\" foreground"

  # Find the correct webhcat-default.xml
  if [ ! -e "$WEBHCAT_DEFAULT_XML" ]; then
    if [[ -n "$PARCELS_ROOT" ]]; then
      # try CDH4.3+ and CDH4.2.1 parcel dirs
      for CANDIDATE in "$PARCELS_ROOT/CDH/etc/"{hive-,}"webhcat/conf.dist/webhcat-default.xml"
      do
        if [ -e "$CANDIDATE" ]; then
          WEBHCAT_DEFAULT_XML="$CANDIDATE"
          break
        fi
      done
    else
      # try CDH4.2.x packages dir
      CANDIDATE="/etc/webhcat/conf.dist/webhcat-default.xml"
      if [ -e "$CANDIDATE" ]; then
          WEBHCAT_DEFAULT_XML="$CANDIDATE"
      fi
    fi
  fi

  # Copy webhcat-default.xml to conf dir
  echo "WEBHCAT_DEFAULT_XML=$WEBHCAT_DEFAULT_XML"
  if !(cp "$WEBHCAT_DEFAULT_XML" "$WEBHCAT_CONF_DIR/"); then
    echo "ERROR: failed to copy webhcat-default.xml"
    exit 1
  fi

  # webhcat-default.xml references PYTHON_CMD and it will die without it
  export PYTHON_CMD="$(which python)"

  if [ -f "$HCAT_PREFIX/share/webhcat/java-client/webhcat-java-client-0.4.0-cdh4.2.0.jar" ]; then
    # CDH4.2.0 has a buggy webhcat_server.sh that fails to kill the real webhcat server
    # trap SIGTERM to nothing, forcing supervisor to do a group SIGKILL, which will correctly clean up all processes
    trap "" SIGTERM
    $WEBHCAT_SERVER_CMD
    # this process is always killed, shouldn't exit here unless WebHCat dies abnormally
    exit $?
  else
    # CDH4.2.1+ webhcat_server.sh does not require special kill handling
    exec $WEBHCAT_SERVER_CMD
  fi

elif [[ "$1" == hiveserver2 ]]; then
  MODE=$2
  if [ -n $MODE ] && [ "${MODE}" == "interactive" ]; then
    #Need to generate kerberos tokens for LLAP daemon lookup.
    #Not renewing while waiting for LLAP daemon launch, as user has other problems if they don't
    #see running LLAP application before this token would expire...
    acquire_kerberos_tgt hive4.keytab
    BACKUP_HADOOP_CLIENT_OPTS=$HADOOP_CLIENT_OPTS
    HADOOP_CLIENT_OPTS=""

    while true;
    do
        result=$($HIVE_HOME/bin/hive --service llapstatus -w -r 1 -i 2 -t 400);
        ret=$?;
        if [ 0 == $ret ]; then
            state=$(echo $result | tr ',' '\n' | grep 'state' | cut -d '"' -f4)
            if [ -n $state ] && [ "${state}" = "RUNNING_ALL" ]; then
              #no-op continue iHS2 startup...
              break
            else
              sleep 10 &
              wait $!
            fi
        else
            echo "Error polling status of LLAP daemons"
            exit 1
        fi
    done
    HADOOP_CLIENT_OPTS=$BACKUP_HADOOP_CLIENT_OPTS
  fi

  if [[ "$SPARK_ON_YARN" == "true" ]]; then
    # Add spark-defaults.properties from spark client config to hive conf dir.
    # Ideally this would be placed in the conf dir by CM, but see OPSAPS-25660.
    # We use a symlink so that if HS2 starts before you've first deployed spark
    # CC, then things still work. HS2 loads spark-defaults.properties each time
    # a client opens a spark session.
    export SPARK_CONF_DIR="${SPARK_CONF_DIR:-/etc/spark/conf}"
    echo "using $SPARK_CONF_DIR as SPARK_CONF_DIR"
    SPARK_FILE="$SPARK_CONF_DIR/spark-defaults.conf"
    if [[ ! -e "$SPARK_FILE" ]]; then
      echo "WARNING: No spark-defaults.properties at $SPARK_FILE. Ensure there is a SPARK_ON_YARN role (such as a Gateway) on this HiveServer2 host and you have deployed Spark's client configuration before running Hive on Spark jobs."
    fi
    if [[ -e "$CONF_DIR/spark-defaults.conf" ]]; then
      echo "WARN: spark-defaults.conf already exists in $CONF_DIR. Skipping symlink creation."
    else
      ln -s "$SPARK_FILE" "$CONF_DIR/spark-defaults.conf"
    fi
  fi
  # check for job secrets keystore to upload
  if [[ -f "$CONF_DIR/$HIVE_JOB_CREDSTORE_SRC" ]]; then
    dir=$(dirname "$HIVE_JOB_CREDSTORE_DEST")

    # Kerberos is enabled here, get/re-new the ticket
    acquire_kerberos_tgt hive4.keytab

    if [ ! $(hadoop fs -test -d "$dir") ]; then
      hadoop fs -mkdir "$dir"
      hadoop fs -chmod 700 "$dir"
    fi

    # Cleanup old job keystore files. We are generating new keystore on every role restart for every HS2 process.
    role_pattern=$(echo $HIVE_JOB_CREDSTORE_DEST | sed -n 's/.*\(jobsecrets-[0-9]\+\).*/\1/p')

    sec_to_day=60*60*24
    curr_ts=$(date +%s)

    for filename in `hadoop fs -ls $dir | awk '{print $NF}' | grep $role_pattern | tr '\n' ' '`; do
      last_acc_ts=$(hadoop fs -stat "%X" $filename)
      last_acc_age=$(( (curr_ts - last_acc_ts/1000)/sec_to_day ))

      if (( $last_acc_age >= $HIVE_JOB_CREDSTORE_TTL )); then
        hadoop fs -rm $filename
      fi
    done

    # Upload new job keystore file.
    hadoop fs -put "$CONF_DIR/$HIVE_JOB_CREDSTORE_SRC" "$HIVE_JOB_CREDSTORE_DEST"
    hadoop fs -chmod 600 "$HIVE_JOB_CREDSTORE_DEST"
  fi

  TEZ_JARS="$PARCELS_ROOT/CDH/jars/tez-*:$PARCELS_ROOT/CDH/lib/tez/*.jar:$CONF_DIR/tez-conf"
  export AUX_CLASSPATH="$AUX_CLASSPATH:$TEZ_JARS"

elif [[ "$1" == updatelocation ]]; then
  NEW_LOC="$2"
  echo "Updating Hive Metastore to use location $NEW_LOC"
  FS_ROOT_OUTPUT=`$HIVE_HOME/bin/hive --config $CONF_DIR --service metatool -listFSRoot`
  FS_ROOT_RETURN=$?

  if [[ 0 != $FS_ROOT_RETURN ]] ; then
    echo "ERROR: Failed while listing FS Root."
    exit 1
  fi

  fs_root_count=0
  echo "List FS Root output:"
  # grep only the db locations starting with hdfs:// and up to the first slash char (not included), and then find the unique values
  echo "$FS_ROOT_OUTPUT" | grep -o "^hdfs://[^/]\+" | sort | uniq |
  while read -r FS_ROOT
  do
    fs_root_count=$((fs_root_count + 1))
    echo "Old FS Root($fs_root_count): $FS_ROOT"
    # FS_ROOT=hdfs://oldhost:8020 or hdfs://oldnameservice

    if [[ "$NEW_LOC" == "$FS_ROOT" ]]; then
      echo "FS Root already matches $NEW_LOC. No Update Required"
      continue
    fi

    $HIVE_HOME/bin/hive --config $CONF_DIR --service metatool -updateLocation "$NEW_LOC" "$FS_ROOT" -tablePropKey avro.schema.url -serdePropKey avro.schema.url
  done
  exit 0

elif [[ "$1" == create_metastore_tables ||  "$1" == upgrade_metastore ]]; then
  if [[ -z "$CMF_SERVER_ROOT" ]]; then
    JDBC_JARS_CLASSPATH="$CLOUDERA_DIR/lib/*:$JDBC_JARS"
  else
    JDBC_JARS_CLASSPATH="$CMF_SERVER_ROOT/lib/*:$JDBC_JARS"
  fi
  if [[ "$1" == create_metastore_tables ]]; then
    if [ "$CDH_VERSION" -ge "5" ]; then
      # CDH 5 uses schema tool to create schema, but it isn't idempotent
      #skip_if_tables_exist metastore_db_py.properties

      # hive config is available to schematool via env var HIVE_CONF_DIR
      exec bash -x $HIVE_HOME/bin/schematool -verbose -dbType $SCHEMATOOL_DBTYPE -initOrUpgradeSchema $POSTGRESQL81_WORKAROUND
    else
      exec $JAVA_HOME/bin/java -Djava.net.preferIPv4Stack=true -cp "$JDBC_JARS_CLASSPATH" com.cloudera.cmf.service.hive.HiveMetastoreDbUtil "$CONF_DIR/metastore_db_py.properties" "$(dirname $0)/ddl" "--createTables"
    fi
  elif [[ "$1" == upgrade_metastore ]]; then
    VERSION=$($JAVA_HOME/bin/java -Djava.net.preferIPv4Stack=true $HIVE_UPGRADE_METASTORE_JAVA_OPTS -cp "$JDBC_JARS_CLASSPATH" com.cloudera.cmf.service.hive.HiveMetastoreDbUtil "$CONF_DIR/metastore_db_py.properties" "" "--printSchemaVersion")
    check_version_run_schematool_upgrade $VERSION
  fi
elif [[ "$1" == validate_metastore ]]; then
  exec bash -x $HIVE_HOME/bin/schematool -verbose -validate -dbType $SCHEMATOOL_DBTYPE $POSTGRESQL81_WORKAROUND
elif [[ "$1" == create_sysdb ]]; then
  if [ "$CDH_VERSION" -ge "7" ]; then
        exec bash -x $HIVE_HOME/bin/schematool -verbose -dbType hive -metaDbType $SCHEMATOOL_DBTYPE -initOrUpgradeSchema $POSTGRESQL81_WORKAROUND
  else
        exec bash -x $HIVE_HOME/bin/schematool -verbose -dbType hive -metaDbType $SCHEMATOOL_DBTYPE -initSchema $POSTGRESQL81_WORKAROUND
  fi
elif [[ "$1" == importRanger ]]; then
  . ${cloudera_config}/cdh-default-hadoop

  set_hadoop_classpath

  if ls ${MGMT_HOME}/lib/dr/authz-main-*.jar; then
    CLASSPATH=$(ls ${MGMT_HOME}/lib/dr/authz-main-*.jar | head -n 1)
    CLASSPATH=${CLASSPATH}:$(ls ${MGMT_HOME}/lib/dr/authz-common-*.jar | head -n 1)
    if [[ -n $(ls ${MGMT_HOME}/lib/dr/authz-exporter-*.jar | head -n 1) ]]; then
      CLASSPATH=${CLASSPATH}:$(ls ${MGMT_HOME}/lib/dr/authz-exporter-*.jar | head -n 1)
    fi
    if [[ -n $(ls ${MGMT_HOME}/lib/dr/authz-ingestor-*.jar | head -n 1) ]]; then
      CLASSPATH=${CLASSPATH}:$(ls ${MGMT_HOME}/lib/dr/authz-ingestor-*.jar | head -n 1)
    fi
    if [[ -n $(ls ${MGMT_HOME}/lib/dr/authz-translator-*.jar | head -n 1) ]]; then
      CLASSPATH=${CLASSPATH}:$(ls ${MGMT_HOME}/lib/dr/authz-translator-*.jar | head -n 1)
    fi
    CLASSPATH=${CLASSPATH}:${CDH_RANGER_ADMIN_HOME}/ews/webapp/WEB-INF/lib/*
    CLASSPATH=${CLASSPATH}:${CDH_HIVE_HOME}/lib/*
    CLASSPATH=${CLASSPATH}:$(hadoop classpath)

    # Kerberos setup
    acquire_kerberos_tgt hive4.keytab

    # Temporary code to detect if sentry-export.json is present or not.
    # Ideally should be done in server with a check exists step,
    # but fixing here as a stop gap.
    f=$(sed -n '/authorization.migration.export.output_file/{n;p}' ${CONF_DIR}/authorization-migration-site.xml)
    if [ -z "$f" ]; then
      echo "Can't determine sentry-export file path"
      exit 1
    fi
    f=$(echo $f | sed -n 's#<value>\(.*\)</value>#\1#p')
    echo "Check if $f exists..."
    hdfs dfs -test -e $f
    if [ $? -eq 1 ]; then
      echo "$f does not exist"
      exit 0
    fi

    # Call the import tool
    exec ${JAVA_HOME}/bin/java -Dlog4j.configuration=file:${CONF_DIR}/authorization-migration-log4j.properties \
        -cp ${CLASSPATH} com.cloudera.enterprise.authzmigrator.Main --command ingest --confdir ${CONF_DIR}
  else
    echo "Sentry Authorization Migration jars do not exist"
    exit 1
  fi
elif [[ "$1" == migrateTables ]]; then
  acquire_kerberos_tgt hive4.keytab
  FS_OPERATION_USER=$2;
  CONTROL_FILE_URL=$3;
  IN_UPGRADE_SESSION=$4;
  # Uncomment for debug options
  # export HADOOP_CLIENT_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005,quiet=y

  if [ -z "$CONTROL_FILE_URL" ]; then
    echo "Invoking HSMM to migrate all tables."
    exec $HIVE_HOME/bin/hive --config $CONF_DIR --service strictmanagedmigration --migrationOption external \
     --fsOperationUser ${FS_OPERATION_USER}
  else
    if [[ "${IN_UPGRADE_SESSION}" == true ]]; then
      echo "Invoking HSMM to migrate all DBs, but none of their tables."
      exec $HIVE_HOME/bin/hive --config $CONF_DIR --service strictmanagedmigration --migrationOption external \
           --fsOperationUser ${FS_OPERATION_USER} --tableRegex "unMatchableTableRegularExpressionToOnlyProcessDbEntitiesAndNoTables"
    else
      echo "Invoking HSMM to migrate DBs and tables as per the supplied control file URL: ${CONTROL_FILE_URL}"
      exec $HIVE_HOME/bin/hive --config $CONF_DIR --service strictmanagedmigration --migrationOption external \
       --fsOperationUser ${FS_OPERATION_USER} --controlFileUrl ${CONTROL_FILE_URL}
    fi
  fi
fi
exec $HIVE_HOME/bin/hive --config $CONF_DIR --service "$@"
