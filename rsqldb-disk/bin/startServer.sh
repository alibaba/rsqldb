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


nohup $JAVA ${JVM_OPTS} -jar ${BASE_DIR}/lib/rsqldb-server-1.0.0-SNAPSHOT.jar > ${BASE_DIR}/log/rsqldb-server.log 2>&1 &

echo "start server success."
