#!/bin/sh

cd `dirname $0`

chmod +x startRunner.sh
chmod +x startServer.sh

sh startRunner.sh

sh startServer.sh

tail -f /etc/hosts



