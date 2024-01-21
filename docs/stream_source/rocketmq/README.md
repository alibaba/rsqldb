# 语法示例

流计算可以将消息队列作为流式数据输入，如下:

```sql
create table metaq_stream
(
    ip varchar header,
    x  varchar,
    y  varchar,
    z  varchar
) with (
      type = 'rocketmq',
      topic = 'mq_topic',
      consumerGroup = 'metaq_group',
      namesrvAddr = '',
      tag = 'mq_tags',
      isJsonData = 'true',
      msgIsJsonArray = ‘ false ’,
      maxThread = '4',
      maxFetchLogGroupSize = 100,
      fieldDelimiter = '#',
      encoding = 'UTF-8'
      );
```

| 参数名                  | 是否必填 | 字段说明                                                                         | 默认值   |
|----------------------|------|------------------------------------------------------------------------------|-------|
| type                 | 是    | 固定值，必须是rocketmq                                                              |       |
| topic                | 是    | 队列的Topic                                                                     |       |
| consumerGroup        | 是    | 消费组名称，可以按规范自取                                                                |       |
| namesrvAddr          | 否    | nameserver 地址，如果配置了[http://jmenv.tbsite.net/](http://jmenv.tbsite.net/)可以不输入 |       |
| tag                  | 否    |                                                                              | *     |
| isJsonData           | 否    | 消息是否是json格式                                                                  | true  |
| msgIsJsonArray       | 否    | 如果消息是jsonarray，设置这个值为true，和isJsonData互斥，isJsonData需要设置成false                 | false |
| maxThread            | 否    | 每个并发用几个线程处理数据                                                                | 1     |
| maxFetchLogGroupSize | 否    | 每次拉取消息的条数                                                                    | 100   |
| fieldDelimiter       | 否    | 把读到的byte[]数组转换成一个string，然后按照行分隔符切分后，再对每一行数据按列分隔符进行分割，按字段顺序来匹配。               |       |
| encoding             | 否    | 字节数组转化成字符串的编码方式                                                              | UTF-8 |

解析的数据来源是拉取的MessageExt的消息 getBody() 内容，目前是对byte[]数组进行解析，默认会基于UTF8转化成字符串，如果isJsonData=true，会再解析成json。
如果要获取到MessageExt.getProperties里面的内容，需要使用header关键字进行标识， 假如用户在MessageExt的成员。

# 属性字段

| 属性字段                | 说明                  |
|---------------------|---------------------|
| TAGS                | 订阅标签                |
| __store_timestamp__ | 消息在broker存储时间       |
| __born_timestamp__  | 消息产生端(producer)的时间戳 |

# 消息堆积&能力扩展

- 支持在运行中，对topic进行分片扩容，当消费能力不足时，建议增加分片输
- 支持在运行中，随时增加并发个数，并发个数不能超过分片个数
- 当消息堆积时，可以通过扩大分片数和并发数解决，不需要重启job

# 自定义解析

当消息非json，jsonarray，或分割符分割，需要业务方自己解析时，可以采用如下方式

```sql
create
grok MyParser  as org.apache.rocketmq.streams.script.annotation.Function;

create table metaq_stream
(
    x varchar
) with (
      type = 'rocketmq',
      topic = 'metaq_topic',
      consumerGroup = 'metaq_group',
      tag = 'metaq_tags',
      isJsonData = 'false',
      encoding = 'UTF-8'
      );

create view paser as
select t.a,
       t.b,
       t.c
from metaq_stream,
     LATERAL table(grok(x,'解析语句')) as t(a,b,c）;

```

- 把isJsonData设置成false，系统会把这个字节数组根据encoding转化成字符串，并赋值给x字段
- 系统提供多种解析函数（类似flink的udtf），包括grok，正则解析等，也可以自定义解析函数，参考自定函数部分，在create view完成字段的解析
