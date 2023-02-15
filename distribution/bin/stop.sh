#!/bin/bash

function_error_exit ()
{
    echo "ERROR: $1 !!"
    exit 1
}

stop()
{
    STR=`ps -ef | grep java | grep -- "rsqldb.jar"`
    [ -z "$STR" ] && return
    kill `ps -ef | grep java | grep -- "rsqldb.jar" | awk '{print $2}'`
}

stop
