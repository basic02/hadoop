#!/bin/bash

# Copyright (c) 2024 Cloudera, Inc. All rights reserved.

#
# Set of utility functions.
#
################################# utility #################################

# Replace {{PID}} in the heap dump path with the process pid
replace_pid() {
  echo $@ | sed "s#{{PID}}#$$#g"
}

# Returns generic java options that should be applied to all
# processes.  The options may be gated on specific JDK versions
# or other environmental conditions.
# Options are return in GENERIC_JAVA_OPTS.
get_generic_java_opts() {
  export GENERIC_JAVA_OPTS=" -Dsun.security.krb5.disableReferrals=true -Djdk.tls.ephemeralDHKeySize=2048 -Dcom.sun.management.jmxremote.ssl.enabled.protocols=TLSv1.2"
}

get_java_major_version() {
  if [ -z $JAVA_HOME/bin/java ]; then
    echo "JAVA_HOME must be set"
    exit 1
  fi
  local VERSION_STRING=`$JAVA_HOME/bin/java -version 2>&1`
  local RE_JAVA='[java|openjdk][[:space:]]version[[:space:]]\"1\.([0-9][0-9]*)\.?+'
  if [[ $VERSION_STRING =~ $RE_JAVA  ]]; then
    eval $1=${BASH_REMATCH[1]}
  else
    RE_JAVA='[java|openjdk][[:space:]]version[[:space:]]\"([0-9][0-9]*)\.?+'
    if [[ $VERSION_STRING =~ $RE_JAVA  ]]; then
      eval $1=${BASH_REMATCH[1]}
    fi
  fi
}

set_basic_gc_tuning_args_based_on_java_version() {
  # fetch java version
  get_java_major_version JAVA_MAJOR
  BASIC_GC_TUNING_ARGS=""
  # populate BASIC_GC_TUNING_ARGS based on java version
  case $JAVA_MAJOR in
    8)   BASIC_GC_TUNING_ARGS="${JAVA8_GC_TUNING_ARGS:-"-XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70 -XX:+CMSParallelRemarkEnabled"}"
         ;;
    11)  BASIC_GC_TUNING_ARGS="${JAVA11_GC_TUNING_ARGS:-}"
         ;;
    *)   echo "Unable to detect JAVA version. Skip using GC tuning args"
         ;;
  esac
}

get_gc_args() {
  # hdfs can supply JVM GC args here [ applicable for CDH >=6.3.0 ]

  GC_LOG_DIR="$(echo $HADOOP_LOG_DIR)"
  GC_DATE="$(date +'%Y-%m-%d_%H-%M-%S')"

  # formulating custom GC args for JAVA8
  JAVA8_VERBOSE_GC_VAR="-Xloggc:$GC_LOG_DIR/gc-$GC_DATE.log -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps"
  JAVA8_GC_LOG_ROTATION_ARGS="-XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=200M"
  JAVA8_GC_TUNING_ARGS="-XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70 -XX:+CMSParallelRemarkEnabled $JAVA8_VERBOSE_GC_VAR $JAVA8_GC_LOG_ROTATION_ARGS"

  # formulating custom GC args for JAVA11
  JAVA11_VERBOSE_GC_VAR="-Xlog:gc:$GC_LOG_DIR/gc-$GC_DATE.log:uptime,level,tags:filecount=10,filesize=200M"
  JAVA11_GC_TUNING_ARGS="-XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70 -XX:+CMSParallelRemarkEnabled $JAVA11_VERBOSE_GC_VAR"

  # provides BASIC_GC_TUNING_ARGS based on java version
  set_basic_gc_tuning_args_based_on_java_version
}

# replace gc args with the second arg to the function call
replace_gc_args() {
  echo $1 | sed "s#{{JAVA_GC_ARGS}}#$2#g"
}

# Acquire Kerberos tgt (ticket-granting ticket) if the server provided the
# principal (in which case the keytab should be non-zero).
#
# Note that we cache it in the current directory so that it will be isolated to
# this hadoop command.
#
# Arguments:
#   $1 - keytab filename
#   $2 - kerberos principal
#   $3 - If set to "true", the given keytab filename is used as-is. If not, we use the default
#   behavior where the keytab file is constructed using ${HDFS_RBF_CONF_DIR}/${1}.
# Returns:
#   None
acquire_kerberos_tgt() {
  if [ -z $1 ]; then
    echo "Must call with the name of keytab file."
    exit 1
  fi

  KERBEROS_PRINCIPAL=$2
  if [ -n "$KERBEROS_PRINCIPAL" ]; then
    if [ -d /usr/kerberos/bin ]; then
      export PATH=/usr/kerberos/bin:$PATH
    fi
    which kinit
    if [ $? -ne 0 ]; then
      echo "kinit does not exist on the host."
      exit 1
    fi

    export KRB5CCNAME=$HDFS_RBF_CONF_DIR/krb5cc_$(id -u)
    echo "using $KERBEROS_PRINCIPAL as Kerberos principal"
    echo "using $KRB5CCNAME as Kerberos ticket cache"
    # Check if we have a fully qualified keytab.
    if [ "$3" = "true" ]; then
      KEYTAB_FILE="${1}"
    else
      KEYTAB_FILE="${HDFS_RBF_CONF_DIR}/${1}"
    fi
    echo "using $KEYTAB_FILE as keytab file"

    kinit -c $KRB5CCNAME -kt $KEYTAB_FILE $KERBEROS_PRINCIPAL
    if [ $? -ne 0 ]; then
      echo "kinit was not successful."
      exit 1
    fi
    # This is work-around for a bug in kerberos >= 1.8 that prevents java
    # programs from reading from the ticket cache. It's harmless to do it
    # unconditionally - as long as we sleep first, in case kerberos is
    # configured to prevent ticket renewal. If the two kinit calls are
    # too close together, the -R can succeed when it shouldn't, and end
    # up expiring the ticket.
    sleep 1
    kinit -R
  fi
}

################################# utility #################################

# Time marker for both stderr and stdout
date; date 1>&2

echo "Running HDFS RBF router script..."
echo "Running HDFS RBF command: $1"

SOURCE="${BASH_SOURCE[0]}"
BIN_DIR="$( dirname "$SOURCE" )"
while [ -h "$SOURCE" ]
do
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$BIN_DIR/$SOURCE"
  BIN_DIR="$( cd -P "$( dirname "$SOURCE"  )" && pwd )"
done
BIN_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

# debug
set -ex

HADOOP_HOME=$BIN_DIR/../lib/hdfs_rbf
export HADOOP_HOME=$(readlink -m "${HADOOP_HOME}")
export HADOOP_HOME_WARN_SUPPRESS=true
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export HADOOP_YARN_HOME=$HADOOP_HOME
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_LIBEXEC_DIR=$HADOOP_HOME/libexec
export JAVA_LIBRARY_PATH=$HADOOP_HOME/lib/native
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-"/etc/hadoop/conf"}
export HDFS_RBF_CONF_DIR=${HDFS_RBF_CONF_DIR:-"/etc/hdfs-rbf/conf"}
export HADOOP_CLASSPATH=$HDFS_RBF_CONF_DIR:$HADOOP_CONF_DIR:$HADOOP_HOME:$HADOOP_HOME/lib/*.jar
export KRB5_CONFIG=${KRB5_CONFIG:-"/etc/krb5.conf"}

export DB_CONNECTOR_JAR_DIR=${DB_CONNECTOR_JAR_DIR:-"/usr/share/java"}
if [[ -d "${DB_CONNECTOR_JAR_DIR}" ]]; then
  export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$DB_CONNECTOR_JAR_DIR/*.jar
fi

export HADOOP_LOG_DIR=${HADOOP_LOG_DIR:-"/var/log/hdfs-rbf"}
export HADOOP_ROOT_LOGGER=${HADOOP_ROOT_LOGGER:-"INFO,RFA"}
export HADOOP_SECURITY_LOGGER=${HADOOP_SECURITY_LOGGER:-"INFO,RFAS"}
export HADOOP_AUDIT_LOGGER=${HADOOP_AUDIT_LOGGER:-"INFO,RFAAUDIT"}
export HOST=$(hostname -f)
export HADOOP_LOGFILE=hadoop-hdfs-dfsrouter-${HOST}.log

ROUTER_JAVA_HEAPSIZE=${ROUTER_JAVA_HEAPSIZE:-"4096"}
ROUTER_JAVA_EXTRA_OPTS=${ROUTER_JAVA_EXTRA_OPTS:-"{{JAVA_GC_ARGS}}"}
HEAP_DUMP_OPTS="-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/hadoop_hdfs_dfsrouter_pid{{PID}}.hprof"
export HADOOP_ROUTER_OPTS="${HADOOP_ROUTER_OPTS} -Xms${ROUTER_JAVA_HEAPSIZE}m -Xmx${ROUTER_JAVA_HEAPSIZE}m ${ROUTER_JAVA_EXTRA_OPTS} ${HEAP_DUMP_OPTS}"
echo "Generated HADOOP_ROUTER_OPTS: ${HADOOP_ROUTER_OPTS}"
export HADOOP_ROUTER_OPTS=$(replace_pid $HADOOP_ROUTER_OPTS)

# Set any generic java options
get_generic_java_opts
export HADOOP_ROUTER_OPTS="${HADOOP_ROUTER_OPTS} ${GENERIC_JAVA_OPTS}"

get_gc_args
ROUTER_GC_ARGS="$BASIC_GC_TUNING_ARGS"

# Now, replace the final GC args within the respective OPTs args
export HADOOP_ROUTER_OPTS=$(replace_gc_args "$HADOOP_ROUTER_OPTS" "$ROUTER_GC_ARGS")
export HADOOP_OPTS="$HADOOP_ROUTER_OPTS $HADOOP_OPTS"

if [ -n $KRB5_CONFIG ]; then
  # HADOOP_OPTS requires this JVM argument to point to the
  # non-default filepath of krb5.conf file as this OPTS is used by $HDFS_BIN
  # $HDFS_BIN is required for the following operations (see below in the script)
  # where the usual OPTS (eg. HADOOP_ROUTER_OPTS) are not used
  export HADOOP_OPTS="-Djava.security.krb5.conf=$KRB5_CONFIG $HADOOP_OPTS"
fi

HDFS_BIN=$HADOOP_HDFS_HOME/bin/hdfs

# Disable IPv6.
export HADOOP_OPTS="-Djava.net.preferIPv4Stack=true $HADOOP_OPTS"

# Calculate full path of keytab file to use.
# Default is hdfs_rbf.keytab in the HDFS_RBF_CONF_DIR; it can be customized via the env var:
# $HDFS_RBF_KEYTAB to override the keytab file
KEYTAB=${HDFS_RBF_KEYTAB:-$HDFS_RBF_CONF_DIR/hdfs_rbf.keytab}
KERBEROS_PRINCIPAL=${HDFS_RBF_PRINCIPAL}

echo "using $JAVA_HOME as JAVA_HOME"
echo "using $HADOOP_HOME as HADOOP_HOME"
echo "using $HADOOP_CONF_DIR as HADOOP_CONF_DIR"
echo "using $HDFS_RBF_CONF_DIR as HDFS_RBF_CONF_DIR"

# if HADOOP_IDENT_STRING is not set, $USER will be used instead. There have
# been situations where $USER is not set for su which results in
# hadoop.id.str ending up to be an empty string so we're setting it here
# explicitly
export HADOOP_IDENT_STRING="hdfs"

# kerberos login
acquire_kerberos_tgt "$KEYTAB" "$KERBEROS_PRINCIPAL" true

if [ "start" = "$1" ]; then
  # Set hadoop security and audit log appenders. These are set here instead
  # of being hardcoded in the log4j template because we only want the hadoop
  # daemons to use them.
  HADOOP_OPTS="-Dsecurity.audit.logger=$HADOOP_SECURITY_LOGGER $HADOOP_OPTS"
  export HADOOP_OPTS="-Dhdfs.audit.logger=$HADOOP_AUDIT_LOGGER $HADOOP_OPTS"

  exec $HDFS_BIN --config $HDFS_RBF_CONF_DIR --daemon start dfsrouter
elif [ "stop" = "$1" ]; then
  exec $HDFS_BIN --config $HDFS_RBF_CONF_DIR --daemon stop dfsrouter
elif [ "status" = "$1" ]; then
  exec $HDFS_BIN --config $HDFS_RBF_CONF_DIR --daemon status dfsrouter
else
  exec $HDFS_BIN --config $HDFS_RBF_CONF_DIR "$@"
fi
