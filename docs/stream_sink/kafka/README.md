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
      type = 'metaq',
      topic = 'metaq_topic',
      group.id = 'kafka_group',
      bootstrap.servers = 'metaq_tags',
      batchSize = 1000,
      autoFlushSize = 300,
      autoFlushTimeGap = 1000
      );
```

| 参数名 | 是否必填 | 字段说明 | 默认值 |
| --- | --- | --- | --- |
| type | 是 | 固定值，必须是metaq | |
| topic | 是 | 队列的Topic |  |
| bootstrap.servers | 否 | kakfa集群 | |
| group.id | 否 | 消费组名称，可以按规范自取 |  |
| batchSize | 否 | 消息缓存输出，缓存的大小 | 1000 |
| autoFlushSize | 否 | 缓存启动线程异步刷新，当缓存条数>配置值时，会刷新缓存 | 300 |
| autoFlushTimeGap | 否 | 缓存启动线程异步刷新，当上次刷新时间到现在时间间隔>配置值时，会刷新缓存 | 1000 |

输出的消息默认是json格式，key是输出表的字段名，value是输出的字段值。整条数据会存在record的value字段 ​

# 环境变量

直接在sql中写参数值，对于专有云，idc输出场景，需要每个用户修改sql，这里提供了环境变量的概念：

- 可以在sql中，设置一个名字给参数，如bootstrap.servers，可以设dipper.streams.kakfa.bootstrap.servers
- 在配置文件中设置dipper.streams.kakfa.bootstrap.servers=真实ak的值
- 系统会自动化检查，检查逻辑：如果属性文件有dipper.streams.kakfa.bootstrap.servers，则用属性的值替换，检查环境变量是否有dipper.streams.kakfa.bootstrap.servers，如果有则用环境变量替换，如果没找到，则认为dipper.streams.kakfa.bootstrap.servers是真实值

```sql
create table metaq_stream
(
    ip varchar header,
    x  varchar,
    y  varchar,
    z  varchar
) with (
      type = 'metaq',
      topic = 'metaq_topic',
      group.id = 'kafka_group',
      bootstrap.servers = 'dipper.streams.kakfa.bootstrap.servers',
      batchSize = 1000,
      autoFlushSize = 300,
      autoFlushTimeGap = 1000
      );
```

kafka支持的环境变量： bootstrap.servers
