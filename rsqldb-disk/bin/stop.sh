#!/bin/sh
set -e
PROG_NAME=$0
JOB_NAMESPACE=$1
JOB_NAMES=$2

# shellcheck disable=SC2068
# shellcheck disable=SC2039

if [ ! -z "${JOB_NAMES}" -a ! -z "${JOB_NAMESPACE}" ]; then
  STREAM_JOB_PIC="$(ps -ef | grep "$JOB_NAMESPACE" | grep "$JOB_NAMES" | grep -v grep | grep -v "$PROG_NAME" | awk '{print $2}' | sed 's/addr://g')"
elif [ ! -z "${JOB_NAMESPACE}" ]; then
  STREAM_JOB_PIC="$(ps -ef | grep "$JOB_NAMESPACE" | grep -v grep | grep -v "$PROG_NAME" | awk '{print $2}' | sed 's/addr://g')"
else
  STREAM_JOB_PIC="$(ps -ef | grep "org.apache.rsqldb.runner.StartAction" | grep -v grep | grep -v "$PROG_NAME" | awk '{print $2}' | sed 's/addr://g')"
fi

if [ ! -z "$STREAM_JOB_PIC" ]; then
  echo $STREAM_JOB_PIC
  echo "Stop rocketmq-streams job"
  echo "kill -9 $STREAM_JOB_PIC"
  kill -9 $STREAM_JOB_PIC
  echo "Job($JOB_NAMES) shutdown completed."
else
  echo "Job($JOB_NAMES) not started."
fi
