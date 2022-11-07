# 使用SDK

同时你可以通过SqlStream可以将实时任务集成到自己的应用中；

## maven依赖

```xml

<dependency>
    <groupId>org.apache.rocketmq</groupId>
    <artifactId>rocketmq-client</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

## 构建SQLStream实例

```java
 SqlStream sqlStream=SqlStreamBuilder.sqlStream("default")
    .memory()
    .init();
```

## 获取集群中任务列表

```java
List<StreamsTask> streamsTaskList=sqlStream.list();
    for(StreamsTask streamsTask:streamsTaskList){
    System.out.println(streamsTask.getConfigureName()+"     "+streamsTask.getState());
    }
```

## 启动实时任务

```java
sqlStream.name("任务名称").start();
```

## 停止实时任务

```java
sqlStream.name("任务名称").stop();
```

## 提交任务

```java
sqlStream.name("任务名称").sql("实时任务sql");
```

## 提交任务(sql文件)

```java
sqlStream.name("任务名称").sqlPath("实时任务sql文件的路径");
```
