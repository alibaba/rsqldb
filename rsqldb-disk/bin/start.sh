#!/bin/sh
set -e

[ ! -e "$JAVA_HOME/bin/java" ] && JAVA_HOME=$HOME/jdk/java
[ ! -e "$JAVA_HOME/bin/java" ] && JAVA_HOME=/usr/java
[ ! -e "$JAVA_HOME/bin/java" ] && error_exit "Please set the JAVA_HOME variable in your environment, We need java(x64)!"

export JAVA_HOME
export JAVA="$JAVA_HOME/bin/java"
export BASE_DIR=$(dirname $0)/..
export CLASSPATH=.:${BASE_DIR}/conf:${BASE_DIR}/lib/*:${CLASSPATH}

JVM_CONFIG="-Xms2048m -Xmx2048m -Xmn1024m"

JAVA_OPTIONS=${JAVA_OPTIONS:-}

JVM_OPTS=()
if [ ! -z "${JAVA_OPTIONS}" ]; then
  JVM_OPTS+=("${JAVA_OPTIONS}")
fi
if [ ! -z "${JVM_CONFIG}" ]; then
  JVM_OPTS+=("${JVM_CONFIG}")
fi

JVM_OPTS="${JVM_OPTS} -cp ${CLASSPATH}"

#JVM_OPTS+=( "-Dlog4j.configuration=$ROCKETMQ_STREAMS_CONFIGURATION/log4j.xml" )

# shellcheck disable=SC2068
# shellcheck disable=SC2039

$JAVA ${JVM_OPTS} org.alibaba.rsqldb.runner.StreamServer ${BASE_DIR}/conf/rsqldb.conf



