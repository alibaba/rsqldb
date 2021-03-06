# 语法

流计算可以将消息队列作为流式数据输入，如下:

```sql
create table es_stream_sink
(
    field1 long,
    field2 varbianary,
    field3 varchar
) with (
      type = 'es',
      endPoint = '',
      port = '',
      needAuth = 'true',
      authUsername = '',
      authPassword = ''
      index = 'mockIdx',
      typeName = 'mockType'
      ...
      );
```

| 参数名 | 是否必填 | 字段说明 | 默认值 |
| --- | --- | --- | --- |
| type | 是 | 固定值，可以是es,elasticsearch |
| endPoint/host | 是 | server 地址，例：[http://127.0.0.1:9211](http://127.0.0.1:9211) |  |
| authUsername | 是 | 如果needAuth=true，需要填写用户名 |  |
| authPassword | 是 | 如果needAuth=true，需要填写密码 |
| needAuth | 是 | 是否需要验证 | false |
| esIndex/index | 是 | 索引名称，类似于数据库 DB 的概念 |  |
| esIndexType/ typeName | 是 | 索引类型 |  |
| socketTimeOut | 否 | 消息缓存输出，缓存的大小 | 1000 |
| connectTimeOut | 否 | 缓存启动线程异步刷新，当缓存条数>配置值时，会刷新缓存 | 300 |
| connectionRequestTimeOut   | 否 | 缓存启动线程异步刷新，当上次刷新时间到现在时间间隔>配置值时，会刷新缓存 | 1000 |
| schema | 否 | schema | log |

# 说明

- 实时计算写入ES原理：针对实时计算每行结果数据缓存，在刷新时，批量写入，能最大的提高吞吐量。
- 如果想提高实时性，可以通过设置batchSize,autoFlushSize,autoFlushTimeGap三个参数来调整。如果

batchSize=1，会一条一输出，性能会大幅下降

- ES测试版本是6.3.2，如果其他版本导致的不兼容，可以基于自定义sink封装实现

# 环境变量

直接在sql中写参数值，既有泄漏的安全风险，对于专有云，idc输出场景，需要每个用户修改sql，这里提供了环境变量的概念：

- 可以在sql中，设置一个名字给参数，如endPoint，可以设dipper.streams.es.endpoint(这个名字随便取)
- 在配置文件中设置dipper.streams.es.endpoint=真实的值
- 系统会自动化检查，检查逻辑：如果属性文件有dipper.streams.es.endpoint，则用属性的值替换，检查环境变量是否有dipper.streams.es.endpoint，如果有则用环境变量替换，如果没找到，则认为dipper.streams.es.endpoint是真实值

```sql
create table es_stream_sink
(
    field1 long,
    field2 varbianary,
    field3 varchar
) with (
      type = 'es',
      endPoint = 'dipper.streams.es.endpoint',
      port = 'dipper.streams.es.port',
      needAuth = 'true',
      authUsername = 'dipper.streams.es.userName',
      authPassword = 'dipper.streams.es.password'
      index = 'dipper.streams.es.index',
      typeName = 'mockType'
      ...
      );
```

es支持的环境变量： endPoint,port,authUsername,authPassword,index 属性文件配置：

```properties
dipper.streams.es.endpoint='',
dipper.streams.es.port='',
dipper.streams.es.userName='',
dipper.streams.es.password=''
dipper.streams.es.index=mockIdx
```
