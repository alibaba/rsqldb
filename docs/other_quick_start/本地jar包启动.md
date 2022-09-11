## jar包启动

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
sh bin/startAll.sh
```

### 配置sql
执行sql以sendDataFromFile.sql为例，该sql任务的数据处理流程为：从本地data.txt文件中读取数据(data.txt文件在client目录中)，过滤出field_1=1的数据，并打印在日志中；为了顺利执行，需要做以下配置：
- client/sendDataFromFile.sql中修改filePath指向data.txt的全路径，例如为：/home/rsqldb/client/data.txt;

### 提交任务
执行路径依然在rsqldb解压目录下
```shell
sh client/clientExector.sh submitTask ${sendDataFromFile.sql文件绝对路径}
```
sendDataFromFile.sql会从本地文件data.txt中读取数据，过滤出只含有field_1=1的数据，并将结果数据输出到日志中。

### 启动任务
在rsqldb解压目录下执行，tail运行日志，为查看结果做准备。
```shell
tail -f log/rsqldb-runner.log
```

另开一个shell窗口，进入解压后的rsqldb目录，执行以下命令启动任务，1分钟后，查看日志输出，会将执行结果打印到日志中。
```shell
sh client/clientExector.sh startTask
```

观察rsqldb-runner.log输出，输出结果中，只包含field_1=1的数据。

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
  sh bin/startAll.sh
```
- 提交任务
```shell
  sh client/clientExector.sh submitTask ${rocketmq.sql文件绝对路径}
```

rocketmq.sql会从RocketMQ的rsqldb-source中读取数据，过滤出field_1=1的数据，并将结果输出到日志文件中。

- 查看输出
```shell
tail -f log/rsqldb-runner.log
```
- 另开一个窗口，启动任务
```shell
sh client/clientExector.sh startTask
```
- 向RocketMQ中生产数据：topic为rsqldb-source，与rocketmq.sql任务中的topic名称保持一致，向该topic写入data.txt文件中的数据。

- 观察输出，在输出结果中，只包含field_1=1的数据。
