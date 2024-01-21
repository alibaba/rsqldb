函数指的是用户自定义函数UDF，Dipper支持3中UDF的应用：

- 兼容blink的udf/udtf/udaf，前提是blink的blink的udf/udtf/udaf中没有用到blink的上下文信息
- Dipper提供的udf/udtf/udaf规范，按Dipper规范书写udf
- 用户已有的java代码，可以发布成udf函数

# 语法：

```sql
CREATE FUNCTION functionName AS className;
```

- blink的udf/udtf/udaf，函数名随便取，className是函数的实现类

```sql
-- udf str.length()
CREATE FUNCTION stringLengthUdf AS 'com.hjc.test.blink.sql.udx.StringLengthUdf';
```

- Dipper提供的udf/udtf/udaf规范，函数名必须是dipper发布的函数名，className是函数的实现类

```sql
-- grok是日志解析函数
CREATE FUNCTION grok AS 'org.apache.rocketmq.streams.script.function.impl.parser.GrokFunction';
```

- 用户已有的java代码，函数名是方法名，className是函数的实现类

# 例子：

```sql
create Function exec_function as 'org.apache.rocketmq.streams.script.annotation.Function';
create Function content_extract as 'org.alicloud.iap.udf.ContentExtract';
create Function extract_test2 as 'org.alicloud.iap.udf.ContentExtract';
create Function extract_test as 'org.alicloud.iap.udf.ContentExtract';

-- 数据源
CREATE TABLE `aegis_inner_exec_bin_es`
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
      type = 'file' --定义维表的变化周期，表明该表是一张会变化的表。
      ,filePath = '/tmp/data_model_extractor_config.txt'
      )
;

--抽取出的中台数据模型
CREATE TABLE `data_model`
(
    `_dm_timestamp`  varchar,
    `_dm_source`     VARCHAR,
    `_dm_type`       VARCHAR,
    `_dm_model_type` VARCHAR,
    `_dm_content`    VARCHAR
)
    with (
        type = 'print'
        ,filePath = '/Users/wurunpeng/Code/intelligent-analysis-platform/data-model/src/main/dipper-task/data-model.txt'
        );


-- 维表测试 unpassed
insert into data_model
select extractor AS _dm_timestamp
    ,_source AS _dm_source
    ,'' AS _dm_type
    ,'' AS _dm_model_type
    ,'' AS content
from
    (
    select
    'aegis_inner_exec_bin_es' AS data_so
        , _ source
    from aegis_inner_exec_bin_es
    ) log join extractor_config FOR SYSTEM_TIME AS OF PROCTIME () AS cfg
on (log.data_so = cfg.data_source or log._ source =cfg.id) and log.data_so = cfg.data_source
;

```
