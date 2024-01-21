#!/bin/sh
source /etc/profile
mvn -U clean install -Dmaven.test.skip=true -DskipTests=true
cp rsqldb-runner/target/rocketmq-streams-distribution.tar.gz docker/rocketmq-streams.tgz
sudo docker build --build-arg APP_NAME=rocketmq-streams --pull -f docker/Dockerfile -t rocketmq-streams .
