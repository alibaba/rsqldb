# 语法示例

流计算可以将消息队列作为流式数据输入，如下:

```sql
create table file_stream
(
    ip varchar header,
    x  varchar,
    y  varchar,
    z  varchar
) with (
      type = 'file',
      filePath = '/tmp/file.txt'
      isJsonData = 'true',
      msgIsJsonArray = ‘ false ’,
      fieldDelimiter = '#'
      );
```

| 参数名            | 是否必填 | 字段说明                                                           | 默认值   |
|----------------|------|----------------------------------------------------------------|-------|
| type           | 是    | 固定值，必须是rocketmq                                                |       |
| filePath       | 是    | 文件路径，文件必须是文本文件，系统默认按行分割                                        |       |
| isJsonData     | 否    | 消息是否是json格式                                                    | true  |
| msgIsJsonArray | 否    | 如果消息是jsonarray，设置这个值为true，和isJsonData互斥，isJsonData需要设置成false   | false |
| fieldDelimiter | 否    | 把读到的byte[]数组转换成一个string，然后按照行分隔符切分后，再对每一行数据按列分隔符进行分割，按字段顺序来匹配。 |       |

文件必须是文本文件，一行一条数据，文件大小不限，系统会分批读取，有内存保护，不会outofmemory。文件source不支持并发，都是单任务执行，可以用来做测试。

# 自定义解析

当消息非json，jsonarray，或分割符分割，需要业务方自己解析时，可以采用如下方式

```sql
create grok MyParser  as org.apache.rocketmq.streams.script.annotation.Function;

create table file_stream
(
    ip varchar header,
    x  varchar,
    y  varchar,
    z  varchar
) with (
      type = 'file',
      filePath = '/tmp/file.txt'
      isJsonData = 'true',
      msgIsJsonArray = ‘ false ’,
      fieldDelimiter = '#'
      );
create view paser as
select t.a,
       t.b,
       t.c
from file_stream,
     LATERAL table(grok(x,'解析语句')) as t(a,b,c）;

```

- 把isJsonData设置成false，系统会把这个字节数组根据encoding转化成字符串，并赋值给x字段
- 系统提供多种解析函数（类似flink的udtf），包括grok，正则解析等，也可以自定义解析函数，参考自定函数部分，在create view完成字段的解析
