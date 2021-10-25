# Rocketmq Streams SQL
Rocketmq Streams SQL 为 Rocketmq Streams 的开发提供了基于SQL的开发体验， 让基于消息队列的流式开发更加容易；


## Features

* 采用标准的流式SQL规范，可以与其他的流计算框架如Flink完美兼容；
* 兼容Flink自带的```udf```、```udaf```和```udtf```，除此之外，用户还可以通过实现相关接口来轻松扩展函数；

## TableStream Example

```java
    import com.alibaba.rsqldb.clients.*;
    String sql="CREATE FUNCTION now as 'com.sql.Function';\n"
                           + "CREATE TABLE graph_vertex_proc (\n"
                           + "  `time` varchar,\n"
                           + "  `uuid` varchar,\n"
                           + "  aliuid varchar,\n"
                           + "  pid varchar,\n"
                           + "  file_path varchar,\n"
                           + "  cmdline varchar,\n"
                           + "  tty varchar,\n"
                           + "  cwd varchar,\n"
                           + "  perm varchar\n"
                           + ") WITH (\n"
                           + " type='metaq',\n"
                           + " topic='blink_dXXXXXXX',\n"
                           + " pullIntervalMs='100',\n"
                           + " consumerGroup='CID_BLINK_SOURCE_001',\n"
                           + " fieldDelimiter='#'\n"
                           + ");\n"
                           + "CREATE TABLE graph_proc_label_extend (\n"
                           + "  `time` varchar,\n"
                           + "  `uuid` varchar,\n"
                           + "  aliuid varchar,\n"
                           + "  pid varchar,\n"
                           + "  file_path varchar,\n"
                           + "  cmdline varchar,\n"
                           + "  tty varchar,\n"
                           + "  cwd varchar,\n"
                           + "  perm varchar\n"
                           + ") WITH (type = 'print');\n"
                           + "INSERT\n"
                           + "  INTO graph_proc_label_extend\n"
                           + "SELECT\n"
                           + "  `time`,\n"
                           + "  `uuid`,\n"
                           + "  aliuid,\n"
                           + "  pid,\n"
                           + "  file_path,\n"
                           + "  cmdline,\n"
                           + "  tty,\n"
                           + "  cwd,\n"
                           + "  perm\n"
                           + "FROM\n"
                           + "  graph_vertex_proc;";
     SQLStreamClient sqlStreamClient= new SQLStreamClient("test_namespace", "test_pipeline",sql);
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