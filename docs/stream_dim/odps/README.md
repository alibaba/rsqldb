# 语法示例

```sql
create table odps_dim
(
    x varchar,
    y varchar,
    z varchar
) with (
      type = 'odps',
      endPoint = '',
      project = '',
      tableName = '',
      partition = '', //可选
      accessId = '',
      accessKey = '',
      cacheTTLMs = '3600000', //可选, 默认 30*1000*60
      fileSize = '128', //可选, 单位byte, 默认 1G
      filePath = '/tmp/xxxx', //可选, 必须为tmp目录, 默认为 /tmp/${project_name}_${tableName}
      isLarge = 'true' //可选, 默认为false。超大维表开启,会使用内存映射方案

      );
```

**字段说明：**

| 字段名        | 是否必须 | 说明         | 默认值                               |  |
|------------|------|------------|-----------------------------------|--|
| type       | Y    | 固定值odps    |                                   |  |
| endPoint   | Y    |            |                                   |  |
| project    | Y    |            |                                   |  |
| tableName  | Y    |            |                                   |  |
| accessId   | Y    |            |                                   |  |
| accessKey  | Y    |            |                                   |  |
| cacheTTLMs | N    |            | 30*1000*60                        |  |
| partition  | N    |            | 默认无分区                             |  |
| fileSize   | N    |            | 1G                                |  |
| filePath   | N    |            | /tmp/${project_name}_${tableName} |  |
| isLarge    | N    | 是否开启内存映射模式 | false                             |  |

# 数据量说明

meta数据 ：对加载的数据做了二进制的压缩，每一行添加行的长度，最多占2个字节，每一列添加一个列长度，最多占用2个字节。 其他同文件维表
​

# 示例

```sql

create table test_source
(
    field_1 varchar,
    field_2 varchar,
    field_3 varchar,
    field_4 varchar,
    field_5 varchar,
    field_6 varchar,
    field_7 varchar,
    field_8 varchar
) with (
      type = 'sls',
      accessId = '',
      logStore = '',
      endPoint = '',
      accessKey = '',
      project = ''
      );

create table test_dim
(
    dim_key varchar,
    filed_1 varchar,
    primary key (dim_key),
    period for system_time
) with (
      type = 'odps',
      endPoint = 'x',
      project = '',
      tableName = '',
      accessId = '',
      accessKey = '',
      cacheTTLMs = ''
      );

create table test_sink
(
    field_1 varchar,
    field_2 varchar,
    field_3 varchar,
    field_4 varchar,
    field_5 varchar,
    field_6 varchar,
    field_7 varchar,
    field_8 varchar
) with (
      type = 'sls',
      endPoint = '',
      accessId = '',
      accessKey = '',
      project = '',
      logStore = ''
      );


insert into test_sink
select field_1
           field_2
    field_3
    field_4
    field_5
    field_6
    field_7
    field_8
from test_source t1
         join
     test_dim for system_time as of proctime() as t2
     on t1.field_1 = t2.dim_key

```
