# rsqldb

rsqldb 为 Rocketmq Streams 的开发提供了基于SQL的开发体验， 让基于消息队列的流式开发更加容易；

## Features

* 采用标准的流式SQL规范，可以与其他的流计算框架如Flink完美兼容；
* 兼容Flink自带的```udf```、```udaf```和```udtf```，除此之外，用户还可以通过实现相关接口来轻松扩展函数；


如果您希望更详细的了解rsqldb的相关内容， 请点击[这里](docs/SUMMARY.md)


## Quickstart
### 运行环境
- JDK 1.8及以上
- Maven 3.2及以上

### 在本地安装RocketMQ-Streams
rsqldb依赖的RocketMQ-Streams版本为1.0.2-preview-SNAPSHOT（RocketMQ-Streams近期会做一次发版，发版之后省略该步骤）。

```shell
git clone https://github.com/apache/rocketmq-streams.git
#切换到main分支
mvn clean install -DskipTest -U
```

### 下载rsqldb工程并本地构建
```xml
git clone https://github.com/alibaba/rsqldb.git

mvn clean package -DskipTest -U
```

### 拷贝安装压缩包并解压

进入rsqldb-disk模块下，将rsqldb-distribution.tar.gz安装包拷贝到任意目录，并执行命令解压并进入解压目录：
```xml
tar -zxvf rsqldb-distribution.tar.gz;cd rsqldb
```


### 启动rsqldb服务端
```shell
chmod +x bin/startAll.sh;sh bin/startAll.sh
```

### 配置sql文件
sendDataFromFile.sql中创建的任务，需要从本地文件指定位置读取数据，所以需要修改sendDataFromFile.sql中filePath变量的位置，修改为数据文件data.txt的绝对路径。


### 提交任务
执行路径依然在rsqldb解压目录下
```shell
chmod +x client/clientExector.sh;sh client/clientExector.sh submitTask sendDataFromFile.sql
```


### 启动任务
在rsqldb解压目录下执行，tail运行日志，为查看结果做准备。
```shell
tail -f log/rsqldb-runner.log
```

另开一个shell窗口，进入解压后的rsqldb目录，执行以下命令启动任务，1分钟后，查看日志输出，会将执行结果打印到日志中。
```shell
sh client/clientExector.sh startTask
```

### 查询任务
在rsqldb解压目录下执行
```shell
sh client/clientExector.sh queryTask
```
返回已经提交的任务列表。

### 停止任务
在rsqldb解压目录下执行
```shell
sh client/clientExector.sh stopTask
```

### 从RocketMQ中读取数据并处理
上述示例为从本地文件data.txt中读取数据，更为常用的用法是从RocketMQ中读取数据处理，下面给出具体步骤：

- 本地安装并启动RocketMQ，[安装文档](https://rocketmq.apache.org/docs/quick-start/)
- 启动rsqldb服务端
```shell
  chmod +x bin/startAll.sh;sh bin/startAll.sh
```
- 提交任务
```shell
  chmod +x client/clientExector.sh;sh client/clientExector.sh submitTask rocketmq.sql
```
- 查看输出
```shell
tail -f log/rsqldb-runner.log
```
- 另开一个窗口，启动任务
```shell
sh client/clientExector.sh startTask
```
- 向RocketMQ中生产数据：topic为rsqldb-source，与rocketmq.sql任务中的topic名称保持一致，向该topic写入data.txt文件中的数据。观察rsqldb-runner.log日志输出。