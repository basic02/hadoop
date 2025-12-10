#!/bin/bash

# Copyright (c) 2024 Cloudera, Inc. All rights reserved.

# Time marker for both stderr and stdout
date; date 1>&2

cloudera_config=/opt/cloudera/cm-agent/service/common
. ${cloudera_config}/cloudera-config.sh

# load the parcel environment
#source_parcel_environment

# attempt to find java
locate_cdh_java_home

# simulate /etc/default/hadoop if necessary
#. ${cloudera_config}/cdh-default-hadoop

export HADOOP_ROUTER_OPTS=$(replace_pid $HADOOP_ROUTER_OPTS)

# Set any generic java options
get_generic_java_opts
export HADOOP_ROUTER_OPTS="${HADOOP_ROUTER_OPTS} ${GENERIC_JAVA_OPTS}"

get_gc_args() {
  # hdfs can supply JVM GC args here [ applicable for CDH >=6.3.0 ]

  # Uncomment below to override GC arguments provided by cloudera-config.sh:
  ##  JAVA8_GC_TUNING_ARGS="GC argument for JAVA8"
  ##  JAVA11_GC_TUNING_ARGS="GC argument for JAVA8"
  GC_LOG_DIR="$(echo $HADOOP_LOG_DIR)"
  GC_DATE="$(date +'%Y-%m-%d_%H-%M-%S')"

  # formulating custom GC args for JAVA8
  JAVA8_VERBOSE_GC_VAR="-Xloggc:$GC_LOG_DIR/gc-$GC_DATE.log -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps"
  JAVA8_GC_LOG_ROTATION_ARGS="-XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=200M"
  JAVA8_GC_TUNING_ARGS="-XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70 -XX:+CMSParallelRemarkEnabled $JAVA8_VERBOSE_GC_VAR $JAVA8_GC_LOG_ROTATION_ARGS"

  # formulating custom GC args for JAVA11
  JAVA11_VERBOSE_GC_VAR="-Xlog:gc:$GC_LOG_DIR/gc-$GC_DATE.log:uptime,level,tags:filecount=10,filesize=200M"
  JAVA11_GC_TUNING_ARGS="-XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70 -XX:+CMSParallelRemarkEnabled $JAVA11_VERBOSE_GC_VAR"

  # function from cloudera-config.sh provides BASIC_GC_TUNING_ARGS based on java version
  set_basic_gc_tuning_args_based_on_java_version
}

get_gc_args
ROUTER_GC_ARGS="$BASIC_GC_TUNING_ARGS"

# If safety valve ENV value is in-use, then user provided GC args are used
[[ ! -z $ROUTER_JVM_GC_ARGS_SAFETY_VALVE ]] && ROUTER_GC_ARGS="$ROUTER_JVM_GC_ARGS_SAFETY_VALVE"

# Now, replace the final GC args within the respective OPTs args
# NOTE: the below replacement only works if CDH >= 6.3.0 [ i.e the version (& future versions) that supports java 11 for CDH ]
export HADOOP_ROUTER_OPTS=$(replace_gc_args "$HADOOP_ROUTER_OPTS" "$ROUTER_GC_ARGS")
export HADOOP_OPTS="$HADOOP_ROUTER_OPTS $HADOOP_OPTS"

if [ -n $KRB5_CONFIG ]; then
  # HADOOP_OPTS requires this JVM argument to point to the
  # non-default filepath of krb5.conf file as this OPTS is used by $HDFS_BIN
  # $HDFS_BIN is required for the following operations (see below in the script)
  # where the usual OPTS (eg. HADOOP_ROUTER_OPTS) are not used
  export HADOOP_OPTS="-Djava.security.krb5.conf=$KRB5_CONFIG $HADOOP_OPTS"
fi

if [ "$CDH_VERSION" -ge "4" ]; then
  HDFS_BIN=$HADOOP_HDFS_HOME/bin/hdfs

  # Disable IPv6. This is automatically done by bin/hadoop for CDH3.
  export HADOOP_OPTS="-Djava.net.preferIPv4Stack=true $HADOOP_OPTS"
elif [ "3" = "$CDH_VERSION" ] || [ "-3" = "$CDH_VERSION" ]; then
  HDFS_BIN=$CDH_HADOOP_HOME/bin/hadoop
  export HADOOP_HOME=$CDH_HADOOP_HOME
  echo "using $HADOOP_HOME as HADOOP_HOME"
else
  echo "ERROR: Unsupported version $CDH_VERSION"
  exit 1
fi

# Calculate full path of keytab file to use.
# Default is hdfs_rbf.keytab in the CONF_DIR; it can be customized via the env var:
# $HDFS_RBF_KEYTAB to override the keytab file (eg. for providesDfs CSDs)
KEYTAB=${HDFS_RBF_KEYTAB:-$CONF_DIR/hdfs_rbf.keytab}

if [ -n "${KERBEROS_AUTH}" ] && [ "${KERBEROS_AUTH}" != "kerberos" ]; then
  # this is necessary in the case of CSDs because they cannot conditionally set SCM_KERBEROS_PRINCIPAL
  # therefore they will need to output also the value of KERBEROS_AUTH which if (only if!) present
  # and not using kerberos => SCM_KERBEROS_PRINCIPAL is removed to prevent triggering other logic in this
  # script and cloudera-config.sh
  unset SCM_KERBEROS_PRINCIPAL
fi

echo "using $JAVA_HOME as JAVA_HOME"
echo "using $CDH_VERSION as CDH_VERSION"
echo "using $CONF_DIR as CONF_DIR"
echo "using $SECURE_USER as SECURE_USER"
echo "using $SECURE_GROUP as SECURE_GROUP"

#set_hadoop_classpath

# debug
set -x

# Search-replace {{CMF_CONF_DIR}} in files
replace_conf_dir

# Make sure topology.py and cloudera_manager_agent_fencer.py (where applicable) are executable
make_scripts_executable

# if HADOOP_IDENT_STRING is not set, $USER will be used instead. There have
# been situations where $USER is not set for su which results in
# hadoop.id.str ending up to be an empty string so we're setting it here
# explicitly
export HADOOP_IDENT_STRING="hdfs"

# kerberos login
acquire_kerberos_tgt "$KEYTAB" "$SCM_KERBEROS_PRINCIPAL" true

if [ "dfsrouter" = "$1" ]; then
  # Set hadoop security and audit log appenders. These are set here instead
  # of being hardcoded in the log4j template because we only want the hadoop
  # daemons to use them.
  HADOOP_OPTS="-Dsecurity.audit.logger=$HADOOP_SECURITY_LOGGER $HADOOP_OPTS"
  export HADOOP_OPTS="-Dhdfs.audit.logger=$HADOOP_AUDIT_LOGGER $HADOOP_OPTS"

  exec $HDFS_BIN --config $CONF_DIR "$@"
else
  exec $HDFS_BIN --config $CONF_DIR "$@"
fi
