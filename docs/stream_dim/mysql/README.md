# 一、语法示例

流计算可以将消息队列作为流式数据输入，如下:

```sql
create table db_dim
(
    x varchar,
    y varchar,
    z varchar
) with (
      type = 'db',
      url = '',
      tableName = '',
      userName = '',
      password = '',
      cacheTTLMs = '30000',
      idFieldName = 'id',
      isLarg = 'true'
      );
```

| 参数名 | 是否必填 | 字段说明 | 默认值 |
| --- | --- | --- | --- |
| type | 是 | 固定值，可以是db,rds | ​
|
| url | 是 | jdbc连接地址 |  |
| tableName | 是 | 既可以是一个表名，如sas_threat，也可以是一个查询条件：from sas_threat where type=1 and productor='dipper'，注意，如果带查询条件，必须加上from |  |
| userName | 是 | 用户名 | ​
|
| password | 是 | 密码 |  |
| cacheTTLMs | 否 | 维表加载时间间隔，单位是毫秒，最少设置为60000，小于60000，会被设置成60000 | 30*1000*60 |
| idFieldName | 否 | 如果设置这个字段，系统会并发多个线程加载数据，提高加载速度，这个字段必须是数字类型，且整体分布不离散 |  |
| isLarg | 否 | 如果设置为true，启用内存映射，部分数据存磁盘，会支持更大存储，会带来性能开销和磁盘占用 | false |

### 说明

- 不需要设置索引，系统会根据join的条件自动创建索引
- 支持inner join和left join，如果匹配多行，会拆分成多行输出。

# 二、类型映射

| RDS/DB字段类型 | 实时计算字段类型 |
| --- | --- |
| BOOLEAN | BOOLEAN |
| TINYINT | TINYINT |
| SMALLINT | SMALLINT |
| INT | INT |
| BIGINT | Lonf |
| FLOAT | FLOAT |
| DECIMAL | DOUBLE |
| DOUBLE | DOUBLE |
| DATE | DATE |
| TIME | Date |
| TIMESTAMP | Date |
| VARCHAR | String |
| VARBINARY | 不支持 |

# 三、数据量

1. 系统最大支持数据量不超过2G，每行数据会有（列数+1)*2个字节的开销，拿一个10个字段，1000w的数据举例，假设每行的数据大小100byte，共需要数据量=(100+11*2)*1000w/1024/1024/1024=1.163g，可以承载
1. 所有数据会存储在内存中，对数据结构做了大量优化，数据量接近原始数据量
1. 索引，系统会根据join条件自动建立索引，根据join中的等值字段建立组合索引，如果一个sql中，对同一维表做了多次join，数据只存一份，会增加多个索引。
1. 索引的数据结构也采用了高压缩内存，一个索引的开销大概是1000w数据330M左右
1. 如果超过2g，设置isLarg=true，会用内存映射文件存储维表

# 四、环境变量

直接在sql中写参数值，既有泄漏的安全风险，对于专有云，idc输出场景，需要每个用户修改sql，这里提供了环境变量的概念：

- 可以在sql中，设置一个名字给参数，如password，可以设dipper.streams.db.password(这个名字随便取)
- 在配置文件中设置dipper.streams.db.password=真实ak的值
- 系统会自动化检查，检查逻辑：如果属性文件有dipper.streams.db.passwords，则用属性的值替换，检查环境变量是否有dipper.streams.db.password，如果有则用环境变量替换，如果没找到，则认为dipper.streams.db.password是真实值


```sql
create table db_dim
(
    ip varchar header,
    x  varchar,
    y  varchar,
    z  varchar
) with (
      type = 'db',
      url = 'dipper.streams.db.url',
      tableName = '',,
      userName = 'dipper.streams.db.userName',
      password = 'dipper.streams.db.password'
      );
```

mysql支持的环境变量： url,userName,password 属性文件配置：

```properties
dipper.streams.db.ur='',
dipper.streams.db.userName='',
dipper.streams.db.password=''
```

# 五、使用方式

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
      type = 'db',
      url = '',
      tableName = '',
      userName = '',
      password = '',
      cacheTTLMs = '30000'
      )
;

select 'aegis_inner_exec_bin_es' AS _source
    ,JSON_VALUE(cfg.extractor, '$._type') AS _type
    ,JSON_VALUE(cfg.extractor, '$._model_type') AS _model_type
from source_data
    join extractor_config FOR SYSTEM_TIME AS OF PROCTIME () AS cfg
on source_data._ source = cfg.data_source
    and data_source=1234 and
    source_data._ type =0 and
    trim (data_source)<source_data._ type
```

- 支持在条件的维表字段使用函数，但此字段不会索引
- 支持非等值比较，建议至少有一个等值比较
- 如果没有不带函数的等值比较，会进行全表的for循环匹配，性能会非常差，请谨慎使用
- 支持inner join和left join


