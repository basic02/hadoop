#!/bin/bash
set -euo pipefail

help ()  {

  echo "Usage: $(basename $BASH_SOURCE) [options]

  Build Hadoop.
  Required parameters:
    -ad|--artifacts-dir              - Artifacts directory
    -pd|--parcel-dir                 - Parcel directory
    -md|--manifest-dir               - Manifest directory
    -hv|--hadoop-version             - Hadoop version
    -fv|--full-version               - Full version
    -mh|--mvn-home                   - Maven home
    -mr|--mvn-repo                   - Maven local repository
    -hsr|--hadoop-source-root        - Hadoop Source Root

  Optional parameters:
    -dc|--distrib-codename           - Distribution codename: el7, el8, ubuntu18, ubuntu20, sles12 (default: el7)
    --gbn                            - GBN
    --official                       - Run build in releng/official environment
    --snapshot                       - Snapshot Build

  -h|--help                          - Show this message"
  exit $1;
}

set_gbn () {
  # Try to obtain GBN only if it's not provided
  if [[ -z "$GBN" ]]; then
    # Let's get the Global Build Number before we do anything else
    GBN=$(curl http://gbn.infra.cloudera.com/)
    if [[ -z "$GBN" ]]; then
      echo "Unable to retrieve Global Build Number. Are you sure you are on VPN?"
      exit 1
    fi
  fi
}

set_mvn () {
  MVN_FLAG="nsu"
  MVN_CMD="-Pdist -Pnative -Dtar -DskipTests -DcreateChecksum=true -Drat.consoleOutput=true -Dhadoop.downstream.gbn=${GBN} -Dmaven.repo.local=${MVN_REPO}"
  MVN="${MVN_HOME}/bin/mvn -B -${MVN_FLAG} ${MVN_CMD}"
}

build_parcel () {
  set_gbn
  set_mvn
  cd ${HADOOP_SOURCE_ROOT} && ${MVN} clean install -Dos.suffix=${DISTRIB_CODENAME}
}

making_directories () {
  mkdir -p ${PARCEL_DIR} ${ARTIFACTS_DIR}/csd/
}

extra_steps () {
  ${MANIFEST_DIR}/make_manifest.py ${HADOOP_SOURCE_ROOT}/cloudera/hdfs-rbf-parcel/target/
  ${HADOOP_SOURCE_ROOT}/cloudera/scripts/filter-maven-artifacts.sh ${MVN_REPO}
}

copy_artifacts () {
  cp -a ${HADOOP_SOURCE_ROOT}/cloudera/hdfs-rbf-parcel/target/HDFS_RBF-${FULL_VERSION}-${GBN}-${DISTRIB_CODENAME}.parcel ${PARCEL_DIR}/
  cp -a ${HADOOP_SOURCE_ROOT}/cloudera/hdfs-rbf-parcel/target/HDFS_RBF-${FULL_VERSION}-${GBN}-${DISTRIB_CODENAME}.parcel.sha1 ${PARCEL_DIR}/
  cp -a ${HADOOP_SOURCE_ROOT}/cloudera/hdfs-rbf-parcel/target/HDFS_RBF-${FULL_VERSION}-${GBN}-${DISTRIB_CODENAME}.parcel.meta.tar.gz ${PARCEL_DIR}/
  cp -r ${HADOOP_SOURCE_ROOT}/cloudera/hdfs-rbf-parcel/target/manifest.json ${PARCEL_DIR}/
  cp -a ${HADOOP_SOURCE_ROOT}/cloudera/hdfs-rbf-csd/target/HDFS_RBF-${FULL_VERSION}-${GBN}.jar ${ARTIFACTS_DIR}/csd/
}

snapshot_build () {
  set_mvn
  cd ${HADOOP_SOURCE_ROOT} && ${MVN} clean deploy -Dos.suffix=${DISTRIB_CODENAME}
}

if [[ $# -eq 0 ]]; then
  help 0
fi

SNAPSHOT='false';
OFFICIAL='false';
DISTRIB_CODENAME='el7';
GBN=

while [[ $# -gt 0 ]];
do
  opt="$1";
    case "$opt" in
      -ad|--artifacts-dir)
        shift
        ARTIFACTS_DIR=$1;;
      -pd|--parcel-dir)
        shift
        PARCEL_DIR=$1;;
      -md|--manifest-dir)
        shift
        MANIFEST_DIR=$1;;
      -hv|--hadoop-version)
        shift
        HADOOP_VERSION=$1;;
      -fv|--full-version)
        shift
        FULL_VERSION=$1;;
      -mh|--mvn-home)
        shift
        MVN_HOME=$1;;
      -mr|--mvn-repo)
        shift
        MVN_REPO=$1;;
      -hsr|--hadoop-source-root)
        shift
        HADOOP_SOURCE_ROOT=$1;;
      -dc|--distrib-codename)
        shift
        DISTRIB_CODENAME=$1;;
      --gbn)
        shift
        export GBN=$1;;
      --official)
        OFFICIAL='true';;
      --snapshot)
        SNAPSHOT='true';;
      -h|--help)
       help 0;;
      *)
      echo "Unknown option: $opt"
      exit 1
      ;;
    esac
  shift
done

if [[ -z "${ARTIFACTS_DIR:-}" ]]; then
  echo -e "Missing parameter: -ad|--artifacts-dir\n"
  help 1
elif [[ -z "${PARCEL_DIR:-}" ]]; then
  echo -e "Missing parameter: -pd|--parcel-dir\n"
  help 1
elif [[ -z "${MANIFEST_DIR:-}" ]]; then
  echo -e "Missing parameter: -md|--manifest-dir\n"
  help 1
elif [[ -z "${HADOOP_VERSION:-}" ]]; then
  echo -e "Missing parameter: -hv|--hadoop-version\n"
  help 1
elif [[ -z "${FULL_VERSION:-}" ]]; then
  echo -e "Missing parameter: -fv|--full-version\n"
  help 1
elif [[ -z "${MVN_HOME:-}" ]]; then
  echo -e "Missing parameter: -mh|--mvn-home\n"
  help 1
elif [[ -z "${MVN_REPO:-}" ]]; then
  echo -e "Missing parameter: -mr|--mvn-repo\n"
  help 1
elif [[ -z "${HADOOP_SOURCE_ROOT:-}" ]]; then
  echo -e "Missing parameter: -hsr|--hadoop-source-root\n"
  help 1
fi

if [[ $SNAPSHOT = 'true' ]]; then
  snapshot_build
else
  build_parcel
  if [[ $OFFICIAL = 'true' ]]; then
    making_directories
    extra_steps
    copy_artifacts
  fi
fi
