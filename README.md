# rsqldb

rsqldb 为 Rocketmq Streams 的开发提供了基于SQL的开发体验， 让基于消息队列的流式开发更加容易；

## Features

* 采用标准的流式SQL规范，可以与其他的流计算框架如Flink完美兼容；
* 兼容Flink自带的```udf```、```udaf```和```udtf```，除此之外，用户还可以通过实现相关接口来轻松扩展函数；


如果您希望更详细的了解rsqldb的相关内容， 请点击[这里](docs/SUMMARY.md)


## Quickstart
### 本地安装docker，[安装链接](https://docs.docker.com/desktop/install/mac-install/)
安装后启动docker

### 下载rsqldb工程
```shell
git clone https://github.com/alibaba/rsqldb.git
```

### 进入工程目录并执行启动docker
```shell
cd rsqldb
docker-compose -f docker-compose.yml up
```

### 进入rsqldb-client容器
#### 提交任务
```shell
sh clientExector.sh submitTask rocketmq.sql
```
任务为向RocketMQ写入数据，过滤出数据中field_1=1的数据；

#### 开始任务
```shell
sh clientExector.sh startTask
```

#### 向RocketMQ中写入数据
```shell
java -cp RocketmqTest-1.0-SNAPSHOT.jar  com.test.rocketmqtest.producer.Producer
```
向RocketMQ的rsqldb-source topic中写入RocketmqTest-1.0-SNAPSHOT.jar包中默认数据，数据如data.txt文件所示。
使用Producer类发送消息时，允许带三个参数：topic、groupId、数据文件全路径，可以向RocketMQ任意topic发送任意数据。
#### 查看结果输出
```shell
java -cp RocketmqTest-1.0-SNAPSHOT.jar  com.test.rocketmqtest.consumer.Consumer
```
从RocketMQ的rsqldb-sink topic中读出结果数据，每执行一次producer会有一行输出(第一次需要等待1min轮询提交的任务)：
```xml
Receive New Messages: body[{"field_3":"3","field_4":"4","field_1":"1","field_2":"2"}]
```
Consumer类允许带两个参数：topic、groupId，可以指定topic消费RocketMQ数据。


## 其他启动方式
### 本地jar包启动
  [本地jar包启动](docs/other_quick_start/本地jar包启动.md)
### 本地工程启动
  [本地工程启动](docs/other_quick_start/本地工程启动.md)


