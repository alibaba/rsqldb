# 语法示例

流计算可以将消息队列作为流式数据输入，如下:

```sql
create table db_dim
(
    x varchar,
    y varchar,
    z varchar
) with (
      type = 'file',
      filePath = '/tmp/dim.txt',
      cacheTTLMs = '30000',
      isLarg = 'true'

      );
```

| 参数名        | 是否必填 | 字段说明                                          | 默认值        |
|------------|------|-----------------------------------------------|------------|
| type       | 是    | 固定值，可以是db,rds                                 |
| filePath   | 是    | 指定file的路径                                     |            |
| cacheTTLMs | 否    | 维表加载时间间隔，单位是毫秒，最少设置为60000，小于60000，会被设置成60000  | 30*1000*60 |
| isLarg     | 否    | 如果设置为true，启用内存映射，部分数据存磁盘，会支持更大存储，会带来性能开销和磁盘占用 | false      |

## 说明

- 文件必须是json数据结构
- file的数据每行一条，每条数据是json结构，系统会把key解析成列，value解析成列值，形成一张表。

# 数据量

1. 系统最大支持数据量不超过2G，每行数据会有（列数+1)
   *2个字节的开销，拿一个10个字段，1000w的数据举例，假设每行的数据大小100byte，共需要数据量=(100+11*2)*
   1000w/1024/1024/1024=1.163g，可以承载
1. 所有数据会存储在内存中，对数据结构做了大量优化，数据量接近原始数据量
1. 索引，系统会根据join条件自动建立索引，根据join中的等值字段建立组合索引，如果一个sql中，对同一维表做了多次join，数据只存一份，会增加多个索引。
1. 索引的数据结构也采用了高压缩内存，一个索引的开销大概是1000w数据330M左右
1. 如果超过2g，设置isLarg=true，会用内存映射文件存储维表

# 使用方式

```sql
create Function exec_function as 'org.apache.rocketmq.streams.script.annotation.Function';
create Function content_extract as 'org.alicloud.iap.udf.ContentExtract';
create Function extract_test2 as 'org.alicloud.iap.udf.ContentExtract';
create Function extract_test as 'org.alicloud.iap.udf.ContentExtract';

-- 数据源
CREATE TABLE `source_data`
(
    `_index`  VARCHAR header,
    `_type`   VARCHAR,
    `_id`     VARCHAR,
    `_score`  VARCHAR,
    `_source` VARCHAR
) with (
      type = 'file',
      filePath = '/tmp/input-log.txt',
      fieldDelimiter = '#'
      );


CREATE TABLE `extractor_config`
(
    data_source varchar,
    id          varchar,
    extractor   varchar
) with (
      type = 'file',
      filePath = '/tmp/dim.txt'
      )
;

select 'source'                                   AS _source
     , JSON_VALUE(cfg.extractor, '$._type')       AS _type
     , JSON_VALUE(cfg.extractor, '$._model_type') AS _model_type
from source_data
         join extractor_config FOR SYSTEM_TIME AS OF PROCTIME() AS cfg
              on source_data._ source = cfg.data_source
    and data_source=1234 and
    source_data._type =0 and
    trim (data_source)<data_source._ type
```

- 支持在条件的维表字段使用函数，但此字段不会索引
- 支持非等值比较，建议至少有一个等值比较
- 如果没有不带函数的等值比较，会进行全表的for循环匹配，性能会非常差，请谨慎使用
- 支持inner join和left join


