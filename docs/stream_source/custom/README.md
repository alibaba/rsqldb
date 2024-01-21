这里的数据源是基于消息队列的push模式的实现类，由消息队列的client实现负载均衡和容错。如果需要自定义分片的负载均衡，容错，需要实现自定义数据源(
pull)部分。

# 引入Maven依赖

```xml

<dependency>
    <groupId>org.apache.rocketmq</groupId>
    <artifactId>rocketmq-streams-commons</artifactId>
    <version>${project.version}</version>
</dependency>
```

- version：和使用的dipper版本保持一致

# 实现子类

继承AbstractSource，实现子类，子类有3个抽象方法：

```java
public class UserDefinedSource extends AbstractSource {

    @Override
    protected boolean startSource() {
        return false;
    }

    @Override
    public boolean supportRemoveSplitFind() {
        return false;
    }

    @Override
    protected boolean isNotDataSplit(String queueId) {
        return false;
    }
}

```

- startSource：启动获取数据的线程，拉取数据，线程个数可以基于maxThread配置。
- supportRemoveSplitFind：如果source在分片变化时，能及时感知，可以设置这个方法返回true，同时在分片删除时，调用父类方法，通知系统分片已经移除。参数splitIds是移除的分片编号，支持多个分片

```java
/**
 * 当分片被移走前需要做的回调
 *
 * @param splitIds 要移走的分片
 */
public void removeSplit(Set<String> splitIds) 
```

- isNotDataSplit，如果有一些系统内部的分片，如做重试用的，这类分片这个方法需要返回true。如rocketmq：

```java
  @Override
protected boolean isNotDataSplit(String queueId){
        return queueId.toUpperCase().startsWith("%RETRY%");
        }
```

# 实现startSource方法

- 当接受到消息时，可以通过以下几个方法把消息转化成JSONObject。
    - 如果接受到数据是byte[]，通过如下方法创建Message，属性headProperties如果没有，直接设置null。

```java
 public JSONObject create(byte[]msg,Map<String, ?> headProperties) 
```

- 如果接受的数据是String，通过如下方法创建Message，属性headProperties如果没有，直接设置null。

```java
JSONObject create(String message,Map<String, ?> headProperties) 
```

- 如果接受的数据是JSONObject,JSONArray或其他类型，通过如下方法创建Message

```java
 public JSONObject createJson(Object message) 
```

- 通过如下方法，把JSONObject转化成Message

```java
  /**
 * 把json 转换成一个message对象
 *
 * @param msg
 * @return
 */
public Message createMessage(JSONObject msg,String queueId,String offset,boolean isNeedFlush) 
```

- msg，上面创建的JSONObject
- queueId，分片id，参考ISplit定义， kakfa是partitionId, sls是shardId。需要唯一，在metaq中queueid构成=brokename+topic+queueid。
- offset，这条消息的offset
- isNeedFlush，是否这条消息强制刷新内存，如果没必要，设置成false
- 通过如下方法，把消息发送出去

```java
public AbstractContext executeMessage(Message channelMessage)
```

# 实现ISplit方法

对消息队列的分片做了抽象，需要实现 ISplit接口，这个接口有两个方法

```java
    String getQueueId();
        /**
         * 获取具体的队列 获取具体的队列
         *
         * @return
         */
        Q getQueue();
```

- getQueueId：需要返回唯一的分片编号，对于metaq这种来说，返回值应该是brokename+topic+queueid。
- getQueue：队列的原始类型，给source使用

# offset commit

- 不同的消息队列，保存offset时机不同，需要在保存offset前，调用sendCheckpoint方法，发送系统通知

```java
/**
 * 发送系统消息，执行组件不可见，告诉所有组件刷新存储
 *
 * @param queueIds
 */
public void sendCheckpoint(Set<String> queueIds)
```

基于metaq 举例

```java
        @Override
public void persistAll(Set<MessageQueue> mqs){
        if(mqs.size()==0){
        return;
        }
        Set<String> queueIds=new HashSet<>();
        for(MessageQueue mq:mqs){
        MetaqMessageQueue metaqMessageQueue=new MetaqMessageQueue(mq);
        queueIds.add(metaqMessageQueue.getQueueId());
        }
        metaqSource.sendCheckpoint(queueIds);
        offsetStore.persistAll(mqs);
        }
```
