这里有两个子类，一个是实现常用的sink，一个是结合source支持shuffle。

# 实现普通Sink

## 引入Maven依赖

```xml
<dependency>
       <groupId>org.apache.rocketmq</groupId>
       <artifactId>rocketmq-streams-commons</artifactId>
       <version>${project.version}</version>
</dependency>
```

- version：和使用的dipper版本保持一致

## 实现子类

继承AbstractUDFSink，实现子类，子类有2个抽象方法：

```java
public class UserDefinedSink extends AbstractUDFSink {

    @Override protected void sendMessage2Store(List<IMessage> messageList) {
        
    }

    @Override protected void sendMessage2Store(ISplit split, List<IMessage> messageList) {

    }
}

```

- 由于sink是可以缓存数据的，当需要刷新到存储时，会调用子类的sendMessage2Store，子类只要实现这个方法即可，尽量使用批量接口，提高吞吐量
- sendMessage2Store(ISplit split, List<IMessage> messageList)，对于消息队列类的消息，可以指定消息发送到哪个队列
- sendMessage2Store(List<IMessage> messageList)，不指定队列，消息基于底层的接口分布数据

# 实现支持shuffle的Sink

在需要支持shuffle的数据源和数据结果表时，需要实现这个方法，数据源和数据结果表必须同时实现 需要继承AbstractSupportShuffleUDFSink，实现子类，子类有个抽象方法

```java
public class UserDefinedSupportShuffleSink extends AbstractSupportShuffleUDFSink {
    @Override public String getShuffleTopicFieldName() {
        return null;
}

    @Override protected void createTopicIfNotExist(int splitNum) {

    }

    @Override public List<ISplit> getSplitList() {
        return null;
}
  
    @Override protected void sendMessage2Store(List<IMessage> messageList) {
        
    }

    @Override protected void sendMessage2Store(ISplit split, List<IMessage> messageList) {

    }
}
```

- getShuffleTopicFieldName，是主题的字段名，如kafka和metaq返回的是topic，sls返回的是logstore‘
- createTopicIfNotExist，在主题没有的时候，基于输入的分片数，创建一个新的主题，如果此主题已经存在，不需要创建。
- getSplitList返回所有的分片信息，并包装成ISplit接口
- sendMessage2Store参考第一章的描述

# 和SQL打通

创建的source和sink要和sql打通，需要实现sql解析的子类，有两个子类，如果需要实现shuffle的能力，继承AbstractChannelSQLSupportShuffleSQLParser子类，如果不需要，继承AbstractChannelSQLParser  子类

## 引入Maven依赖

```xml
<dependency>
       <groupId>org.apache.rocketmq</groupId>
       <artifactId>rocketmq-streams-commons</artifactId>
       <version>${project.version}</version>
</dependency>
```

## 实现普通的sink和source

- 继承AbstractChannelBuilder子类

```java

@AutoService(IChannelBuilder.class)
@ServiceName(value = UDFDefinedSQLParser.TYPE, aliasName = "source_alias_name")
public class UDFDefinedSQLParser extends AbstractChannelSQLParser {
    public static final String TYPE="source_type";
    @Override
    protected String getSourceClass() {
            return null;
    }

    @Override protected String getSinkClass() {
        return null;
}
}

```

- 需要两个标注:
    - ServiceName，可以取两个名字，这个名字对应sql中的type属性;
    - @AutoService(IChannelBuilder.class),这个标准，可以让系统自动加载实现类
- getSourceClass，返回source的Class对象
- getSinkClass，返回sink的Class对象

## 实现支持shuffle的sink和source

- 继承AbstractSupportShuffleChannelBuilder子类

```java
@AutoService(IChannelBuilder.class)
@ServiceName(value = UDFDefinedSQLParser.TYPE, aliasName = "source_alias_name")
public class UDFDefinedSQLParser extends AbstractChannelSQLSupportShuffleSQLParser {
    public static final String TYPE="source_type";

    @Override
protected String getSourceClass() {
        return null;
}

    @Override protected String getSinkClass() {
        return null;
}

    @Override public ISink createBySource(ISource pipelineSource) {
        return null;
}
}
```

除了上面的标准和getSourceClass，getSinkClass方法，还需要实现createBySource这个方法，这个方法是根据一个source对象，创建一个sink对象出来
