#!/bin/sh
set -e

[ ! -e "$JAVA_HOME/bin/java" ] && JAVA_HOME=$HOME/jdk/java
[ ! -e "$JAVA_HOME/bin/java" ] && JAVA_HOME=/usr/java
[ ! -e "$JAVA_HOME/bin/java" ] && error_exit "Please set the JAVA_HOME variable in your environment, We need java(x64)!"

export JAVA_HOME
export JAVA="$JAVA_HOME/bin/java"
export BASE_DIR=$(dirname $0)/..
export CLASSPATH=.:${BASE_DIR}/conf:${BASE_DIR}/server/*:${BASE_DIR}/client/*:${CLASSPATH}

JVM_CONFIG="-Xms1024m -Xmx1024m -Xmn256m"

JAVA_OPTIONS=${JAVA_OPTIONS:-}

JVM_OPTS=()
if [ ! -z "${JAVA_OPTIONS}" ]; then
  JVM_OPTS+=("${JAVA_OPTIONS}")
fi
if [ ! -z "${JVM_CONFIG}" ]; then
  JVM_OPTS+=("${JVM_CONFIG}")
fi

JVM_OPTS+=( "-Dlog4j.configuration=${BASE_DIR}/conf/log4j.xml" )

JVM_OPTS="${JVM_OPTS} -cp ${CLASSPATH}"



# shellcheck disable=SC2068
# shellcheck disable=SC2039

nohup $JAVA ${JVM_OPTS} org.alibaba.rsqldb.runner.StreamRunner $1 > ${BASE_DIR}/log/rsqldb-runner.log &

echo "start runner success."

