#!/bin/sh
set -e

binDir=$(cd `dirname $0`;pwd)
echo "binDir=$binDir"

cd $binDir/..
homeDir=$(pwd)

echo "homeDir=$homeDir"

cd $homeDir
if [ ! -d log ]; then
  mkdir log
fi

path=$homeDir/dipper.cs

sysOS=`uname -s`

if [ $sysOS == "Darwin" ];then
  sed -i '' 's#^filePathAndName.*$#filePathAndName='$path'#g'  $homeDir/conf/rsqldb.conf;
elif [ $sysOS == "Linux" ];then
	sed -i 's#^filePathAndName.*$#filePathAndName='$path'#g'  $homeDir/conf/rsqldb.conf;
fi

cd $binDir

chmod +x startTaskExecutor.sh
chmod +x startSqlReceiver.sh



sh startTaskExecutor.sh $homeDir

sh startSqlReceiver.sh $homeDir




