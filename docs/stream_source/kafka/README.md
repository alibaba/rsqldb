# 语法示例

流计算可以将消息队列作为流式数据输入，目前系统引入的kafka版本2.12，kakfa不同版本的接口有差异，如果需要用到其他版本，可以参考自定义source部分，自己封装，如下:

```sql
create table kafka_stream
(
    ip varchar header,
    x  varchar,
    y  varchar,
    z  varchar
) with (
      type = 'kafka',
      topic = 'kafka_topic',
      group.id = 'kafka_group',
      bootstrap.servers = '',
      isJsonData = 'true',
      msgIsJsonArray = ‘ false ’,
      maxThread = '4',
      maxFetchLogGroupSize = '100',
      fieldDelimiter = '#',
      encoding = 'UTF-8'
      );
```

| 参数名 | 是否必填 | 字段说明 | 默认值   |
| --- | --- | --- |---|
| type | 是 | 固定值，必须是metaq ||
| topic | 是 | 队列的Topic |       |
| group.id/consumerGroup | 是 | 消费组名称，可以按规范自取，用group.id或consumerGroup都可以 |       |
| bootstrap.servers | 是 | kafka集群地址 |       |
| isJsonData | 否 | 消息是否是json格式 | true  |
| msgIsJsonArray | 否 | 如果消息是jsonarray，设置这个值为true，和isJsonData互斥，isJsonData需要设置成false | false |
| maxThread | 否 | 每个并发用几个线程处理数据 | 1     |
| maxFetchLogGroupSize | 否 | 每次拉取消息的条数 | 100   |
| fieldDelimiter | 否 | 把读到的byte[]数组转换成一个string，然后按照行分隔符切分后，再对每一行数据按列分隔符进行分割，按字段顺序来匹配。 |       |
| encoding | 否 | 字节数组转化成字符串的编码方式 | UTF-8 |

解析的数据来源是拉取的ConsumerRecords的消息 value() 内容，原始消息是byte[],会基于用户传入的encoding解析成字符串，如果isJsonData=true，会再解析成json。如果配置了fieldDelimiter，会基于分割符拆分字符串，按字段输入顺序形成行数据。

# 和blink区别

- 不要求table的字段必须是如下字段和保持字段顺序，可以直接对核心的messag字段定义解析格式，如json，分割符拆分等。如果需要自定义解析，isJsonData设置成false，fieldDelimiter不设置即可。此时只声明一个字符串对象即可
- blink要求的其他字段均放到属性中，可以标记head获取

```sql
create table metaq_stream
(
    msg   varchar,
    topic varchar header
) with (
      type = 'kafka',
      topic = 'kafka_topic',
      group.id = 'kafka_group',
      bootstrap.servers = 'metaq_tags',
      isJsonData = 'false'
      );
```

# 属性字段

| 属性字段 | 说明 |
| --- | --- |
| messageKey  | message key |
| topic | topic |
| partition | 分区 |
| offset | offset |
| timestamp | message 时间 |

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
    msg   varchar,
    topic varchar header
) with (
      type = 'kafka',
      topic = 'kafka_topic',
      group.id = 'kafka_group',
      bootstrap.servers = 'kafak.server',
      isJsonData = 'false'
      );

create view paser as
select t.a,
       t.b,
       t.c
from metaq_stream,
     LATERAL table(grok(msg,'解析语句')) as t(a,b,c）;

```

- 把isJsonData设置成false，系统会把这个字节数组根据encoding转化成字符串，并赋值给x字段
- 系统提供多种解析函数（类似flink的udtf），包括grok，正则解析等，也可以自定义解析函数，参考自定函数部分，在create view完成字段的解析

# 环境变量

直接在sql中写参数值，对于专有云，idc输出场景，需要每个用户修改sql，这里提供了环境变量的概念：

- 可以在sql中，设置一个名字给参数，如bootstrap.servers，可以设dipper.streams.kakfa.bootstrap.servers
- 在配置文件中设置dipper.streams.kakfa.bootstrap.servers=真实ak的值
- 系统会自动化检查，检查逻辑：如果属性文件有dipper.streams.kakfa.bootstrap.servers，则用属性的值替换，检查环境变量是否有dipper.streams.kakfa.bootstrap.servers，如果有则用环境变量替换，如果没找到，则认为dipper.streams.kakfa.bootstrap.servers是真实值


```sql
create table kafka_stream
(
    ip varchar header,
    x  varchar,
    y  varchar,
    z  varchar
) with (
      type = 'kafka',
      topic = 'kafka_topic',
      bootstrap.servers = 'kafka_group',
      bootstrap.servers = 'dipper.streams.kakfa.bootstrap.servers',
      isJsonData = 'true',
      msgIsJsonArray = ‘ false ’,
      maxThread = '4',
      maxFetchLogGroupSize = '100',
      fieldDelimiter = '#',
      encoding = 'UTF-8'
      );
```

kafka支持的环境变量： bootstrap.server 属性文件配置：

```properties
dipper.streams.kakfa.bootstrap.servers=
```
