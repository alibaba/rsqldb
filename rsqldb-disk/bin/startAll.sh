#!/bin/sh

binDir=$(cd `dirname $0`;pwd)
echo "binDir=$binDir"

cd $binDir/..
homeDir=$(pwd)

echo "homeDir=$homeDir"

cd $binDir

chmod +x startRunner.sh
chmod +x startServer.sh



sh startRunner.sh $homeDir

sh startServer.sh $homeDir




