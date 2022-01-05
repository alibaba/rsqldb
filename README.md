# Rocketmq Streams SQL

Rocketmq Streams SQL 为 Rocketmq Streams 的开发提供了基于SQL的开发体验， 让基于消息队列的流式开发更加容易；

## Features

* 采用标准的流式SQL规范，可以与其他的流计算框架如Flink完美兼容；
* 兼容Flink自带的```udf```、```udaf```和```udtf```，除此之外，用户还可以通过实现相关接口来轻松扩展函数；

## TableStream Example

```java
    import com.alibaba.rsqldb.clients.*;
    String sql="CREATE FUNCTION now as 'com.sql.Function';\n"
        +"CREATE TABLE source_table (\n"
        +"  field1 varchar,\n"
        +"  field2 varchar,\n"
        +"  field3 varchar,\n"
        +"  field4 varchar,\n"
        +"  field5 varchar,\n"
        +"  field6 varchar,\n"
        +"  field7 varchar\n"
        +") WITH (\n"
        +" type='metaq',\n"
        +" topic='TOPIC_01',\n"
        +" pullIntervalMs='100',\n"
        +" consumerGroup='CONSUMER_GROUP',\n"
        +" fieldDelimiter='#'\n"
        +");\n"
        +"CREATE TABLE sink_table (\n"
        +"  field1 varchar,\n"
        +"  field2 varchar,\n"
        +"  field3 varchar,\n"
        +"  field4 varchar,\n"
        +"  field5 varchar,\n"
        +"  field6 varchar,\n"
        +"  field7 varchar\n"
        +") WITH (type = 'print');\n"
        +"INSERT\n"
        +"  INTO sink_table\n"
        +"SELECT\n"
        +"  field1,\n"
        +"  field2,\n"
        +"  field3,\n"
        +"  field4,\n"
        +"  field5,\n"
        +"  field6,\n"
        +"  field7\n"
        +"FROM\n"
        +"  source_table;";
    
        SQLStreamClient sqlStreamClient = new SQLStreamClient("test_namespace","test_pipeline",sql);
        sqlStreamClient.start();

```

## Maven Repository

```xml

<dependency>
    <groupId>com.alibaba</groupId>
    <artifactId>rsqldb-clients</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

## 独立运行

rsqldb 作为sql版本的rocketmq-streams 框架， 支持俩种运行方式，一种是集成在自己的应用中使用， 方法如上；另外一种则是独立运行，具体方法如下

### 打包

+ 在源码的根目录下运行```mvn clean install```， 对源码进行编译打包
+ 从```rsqldb-runner/target```目录下获取rocketmq-streams-{version}-distribution.tar.gz 并解压
+ 目录结果如下


+ bin 相关指令，包括start.sh 和 stop.sh
    + start.sh 启动指令
    + stop.sh 停止指令
+ conf 配置目录，包括dipper.properties 以及log4j.xml
    + dipper.properties 用于精细化控制应用执行中的各种参数
    + log4j.xml 用于控制日志的输出等级以及输出方式
+ jobs 实时的sql任务存放于此
+ lib 任务依赖的相关jar包
+ log 任务运行产生的各种日志
+ README.md 说明文档
+ LICENSE 许可证
+ NOTICE 声明

### jobs目录

在jobs目录中，任务是以```.sql``` 文件存在的， 每一个独立的sql文件就是一个实时任务


jobs目录最多俩层，如果sql文件直接放在jobs目录下， 则该文件的名称就是任务的namespace以及任务的名称； jobs目录可以有第二层目录， sql文件也可以放在这一层目录中，此时文件夹的名称就是namespace的名称，而文件夹中目录的名称为任务的名称；

## 任务的启动、停止和恢复

任务可以通过bin目录中的start.sh 和stop.sh对任务进行启动和停止；

### 任务启动

```shell
#启动过程中不加任务参数，则会将jobs中所有的任务都同时启动
bin/start.sh

#启动过程加入第一个参数，即namespace参数， 则会将jobs目录下， namespace子目录下的任务都同时启动
bin/start.sh namespace

# 启动过程中如果添加了namespace 和 任务名称， 则系统会在jobs目录中查询相关同名任务并启动，即便是
# 存在于不同的namespace的同名任务
bin/start.sh namespace job_name

# 用户可以给每个任务去配置不同的jvm 参数
bin/start.sh namespace job_name  '-Xms2048m -Xmx2048m -Xss512k'
```

同时，rocketmq-streams框架还支持指定任务脚本目录，来启动实时任务

```shell
#指定sql脚本的文件路径，启动实时任务， 此时任务的namespace和任务名称都与文件名相同
bin/start-sql.sh sql_file_path

#除了指定sql脚本的文件路径，还指定了namespace， 此时任务使用指定的namespace来启动
bin/start-sql.sh sql_file_path namespace

# 启动过程中如果添加了namespace和任务名称，则任务使用指定的namespace和job_name来启动
bin/start.sh sql_file_path namespace job_name

# 用户可以给每个任务去配置不同的jvm 参数
bin/start.sh sql_file_path namespace job_name  '-Xms2048m -Xmx2048m -Xss512k'

```

### 任务停止

```shell
# 停止过程不加任何参数，则会将目前所有运行的任务同时停止
bin/stop.sh

# 停止过程加namespace参数，则会将该namespace目录下的所有任务同时停止
bin/stop.sh namespace

# 停止过程添加了namespace 和 任务名称， 则会将目前运行的所有同名的任务都全部停止
bin/stop.sh namespace job_name
```

### 任务恢复

基于sql脚本编译后的中间结果对任务进行恢复， 中间结果存储的类型以及相关的配置有dipper.properties中的相关项来设置，当存储类型为memory时，任务是无法恢复的

```shell
# 恢复具体的某个job
bin/recover.sh namespace jobname

# 恢复某个namespace下所有的job
bin/recover.sh namespace

```

## 日志查看

目前所有的运行日志都会存储在 ```log/catalina.out```文件中

## 任务配置

conf目录下有俩个有来个重要的配置，一个是```log4j.xml```， 主要用来配置日志等级等， ```dipper.properties``` 则是用来配置任务运行中的相关参数的。

用户可以通过配置dipper.properties 文件来设定任务运行过程中的各种特征，如指纹设置， state的存储设置等；

```properties
## checkpoint存储配置，可以是memory, DB 或者file， 除了checkpoint外， 任务序列化的内容也会被缓存在该存储
# dipper.configurable.service.type=memory
## 当checkpoint为DB时
# dipper.rds.jdbc.type=
# dipper.rds.jdbc.url=
# dipper.rds.jdbc.username=
# dipper.rds.jdbc.password=
# dipper.rds.jdbc.driver=com.mysql.jdbc.Driver
# dipper.rds.table.name=dipper_configure
## 任务从存储反序列化的频次
# dipper.configurable.polling.time=60   #单位秒(s)
## 监控日志的相关配置
# dipper.monitor.output.level=INFO #日志等级，有三种INFO，SLOW，ERROR
# dipper.monitor.slow.timeout=60000 #慢查询超时时间
# dipper.monitor.logs.dir=./logs #日志目录
## 窗口配置
# dipper.window.join.default.interval.size.time=  #join的默认窗口大小
# dipper.window.join.default.retain.window.count= #需要保留几个窗口
# dipper.window.default.fire.delay.second=         #窗口延迟多长时间触发
# dipper.window.default.interval.size.time=       #统计默认的窗口大小，单位是分钟。默认是滚动窗口，大小是1个小时
# dipper.window.default.time.unit.adjust=         #统计默认的窗口大小，单位是分钟。默认是滚动窗口，大小是1个小时
# dipper.window.over.default.interval.size.time=  #over partition窗口的默认时间
## 窗口shuffle配置
# window.shuffle.channel.type=rocketmq #shuffle使用的数据存储类型
# window.shuffle.channel.topic=        #根据type的配置，有不同的配置， topic是指用于shuffle的rocketmq的topic；
# window.shuffle.channel.tags=         #根据type的配置，有不同的配置， tags是指用于shuffle的rocketmq的tags；
# window.shuffle.channel.group=        #根据type的配置，有不同的配置， group是指用于shuffle的rocketmq的group；
# window.system.message.channel.owner=#如果能做消息过滤，只过滤本window的消息，可以配置这个属性，如rocketmq的tags.不支持的会做客户端过滤
## 自定义配置文件路径， 默认查找classpath下的配置文件，或者在jar包所在目录的资源文件
# filePathAndName=classpath://dipper.cs
## 把所有的输出重新定向，当测试时，不需要把结果写入正式的输出时，可以使用，默认会打印，如果需要输出到其他存储，可以配置#
# out.mock.switch=false
## mock的类型，可以是print，metaq和sls。下面是具体类型的配置
# out.mock.type=print
# out.mock.metaq.topic=
# out.mock.metaq.tag=
# out.mock.metaq.consumerGroup=
# out.mock.sls.endPoint=
# out.mock.sls.project=
# out.mock.sls.logstore=
# out.mock.sls.accessKey=
# out.mock.sls.accessId=
# out.mock.sls.group=
```

如果您希望更详细的了解Rsqldb的相关内容， 请点击[这里](docs/SUMMARY.md)

