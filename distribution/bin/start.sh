#!/bin/sh

binDir=$(cd `dirname $0`;pwd)
echo "binDir=$binDir"

cd $binDir/..
homeDir=$(pwd)
echo "homeDir=$homeDir"
cd $homeDir

confPath=$homeDir/conf/rsqldb.conf
if [ ! -z "$1" ]; then
  confPath=$1
fi
echo "confPath=$confPath"

if [ ! -d log ]; then
  mkdir log
fi

[ ! -e "$JAVA_HOME/bin/java" ] && JAVA_HOME=$HOME/jdk/java
[ ! -e "$JAVA_HOME/bin/java" ] && JAVA_HOME=/usr/java
[ ! -e "$JAVA_HOME/bin/java" ] && error_exit "Please set the JAVA_HOME variable in your environment, We need java(x64)!"

export JAVA_HOME
export JAVA="$JAVA_HOME/bin/java"
export BASE_DIR=$(dirname $0)/..
export CLASSPATH=.:${BASE_DIR}/conf:${BASE_DIR}/server/*

JVM_CONFIG="-Xms512m -Xmx512m -Xmn128m"

JAVA_OPTIONS=${JAVA_OPTIONS:-}

JVM_OPTS=()
if [ ! -z "${JAVA_OPTIONS}" ]; then
  JVM_OPTS+=("${JAVA_OPTIONS}")
fi

if [ ! -z "${JVM_CONFIG}" ]; then
  JVM_OPTS+=("${JVM_CONFIG}")
fi

JVM_OPTS="${JVM_OPTS} -cp ${CLASSPATH}"

nohup $JAVA ${JVM_OPTS} -jar $homeDir/server/rsqldb.jar $confPath > /dev/null 2>&1 &

echo "start server success."

