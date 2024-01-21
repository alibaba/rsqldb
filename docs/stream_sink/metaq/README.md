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
      consumerGroup = 'metaq_group',
      tag = 'metaq_tags',
      batchSize = 1000,
      autoFlushSize = 300,
      autoFlushTimeGap = 1000
      );
```

| 参数名              | 是否必填 | 字段说明                                 | 默认值  |
|------------------|------|--------------------------------------|------|
| type             | 是    | 固定值，必须是metaq                         |      |
| topic            | 是    | 队列的Topic                             |      |
| producerGroup    | 否    | 消费组名称，可以按规范自取                        |      |
| tag              | 否    |                                      | *    |
| batchSize        | 否    | 消息缓存输出，缓存的大小                         | 1000 |
| autoFlushSize    | 否    | 缓存启动线程异步刷新，当缓存条数>配置值时，会刷新缓存          | 300  |
| autoFlushTimeGap | 否    | 缓存启动线程异步刷新，当上次刷新时间到现在时间间隔>配置值时，会刷新缓存 | 1000 |

输出的消息默认是json格式，key是输出表的字段名，value是输出的字段值，metaq刷新用的接口是sendOneway。如果系统崩溃，可能会有数据的丢失
