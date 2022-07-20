#!/bin/sh

binDir=$(cd `dirname $0`;pwd)

cd $binDir/..
homeDir=$(pwd)

cd $binDir


mainClass=()
if [ "x$1" == "xsubmitTask" ]; then
    mainClass=com.alibaba.rsqldb.client.SubmitTask
    java -cp rsqldb-client.jar ${mainClass} ${homeDir} $2
fi

if [ "x$1" == "xstartTask" ]; then
    mainClass=com.alibaba.rsqldb.client.StartTask
    java -cp rsqldb-client.jar ${mainClass}
fi

if [ "x$1" == "xqueryTask" ]; then
    mainClass=com.alibaba.rsqldb.client.QueryTask
    java -cp rsqldb-client.jar ${mainClass}
fi

if [ "x$1" == "xstopTask" ]; then
    mainClass=com.alibaba.rsqldb.client.StopTask
    java -cp rsqldb-client.jar ${mainClass}
fi

