# SLS说明

日志服务本身是流数据存储，流计算能将其作为流式数据输入。对于日志服务而言，每条数据类似一个JSON格式，举例如下：

```sql
{
	"a": 1000,
	"b": 1234,
	"c": "li"
}
```

# 语法示例

流计算可以将消息队列作为流式数据输入，如下:

```sql
create table sls_stream
(
    ip varchar header,
    x  varchar,
    y  varchar,
    z  varchar
) with (
      type = 'sls',
      endPoint = '',
      accessId = '',
      accessKey = '',
      project = '',
      logStore = '',
      producerGroup = '',
      batchSize = 1000,
      autoFlushSize = 300,
      autoFlushTimeGap = 1000
      );
```

| 参数名 | 是否必填 | 字段说明 | 默认值      |
| --- | --- | --- |----------|
| type | 是 | 固定值，必须是sls |          |
| endPoint | 是 | 消费端点信息 |          |
| accessId | 是 | sls读取的accessKey |          |
| accessKey | 是 | accessKey | sls读取的密钥 |
| project | 是 | 读取的sls项目 ||
| logStore | 是 | project下的具体的logStore ||
| producerGroup | 否 | 消费组名 ||
| maxThread | 否 | 一个并发任务启动几个线程 |          |
| batchSize | 否 | 消息缓存输出，缓存的大小 | 1000     |
| autoFlushSize | 否 | 缓存启动线程异步刷新，当缓存条数>配置值时，会刷新缓存 | 300      |
| autoFlushTimeGap | 否 | 缓存启动线程异步刷新，当上次刷新时间到现在时间间隔>配置值时，会刷新缓存 | 1000     |

输出的消息默认是json格式，key是输出表的字段名，value是输出的字段值，metaq刷新用的接口是sendOneway。如果系统崩溃，可能会有数据的丢失

# 环境变量

直接在sql中写ak，sk容易带来安全风险，同时对于专有云，idc输出场景，需要每个用户修改sql，这里提供了环境变量的概念：

- 可以在sql中，设置一个名字给参数，如accessId，设置dipper.streams.ak，而非真实值
- 在配置文件中设置dipper.streams.ak=真实ak的值
- 系统会自动化检查，检查逻辑：如果属性文件有dipper.streams.ak，则用属性的值替换，检查环境变量是否有dipper.streams.ak，如果有则用环境变量替换，如果没找到，则认为dipper.streams.ak是真实值

```sql
create table sls_stream
(
    ip varchar header,
    x  varchar,
    y  varchar,
    z  varchar
) with (
      type = 'sls',
      endPoint = 'dipper.streams.endPoint',
      accessId = 'dipper.streams.ak',
      accessKey = 'dipper.streams.sk',
      project = 'dipper.streams.project',
      logStore = 'dipper.streams.logstore',
      consumerGroup = 'consumerGroupTest1'
      maxThread = '4'
      );
```

sls支持的环境变量：endPoint，accessId，accessKey，project，logStore 属性文件配置：

```properties
dipper.streams.endPoint=
dipper.streams.ak=
dipper.streams.sk=
dipper.streams.project=
dipper.streams.logstore=
```
