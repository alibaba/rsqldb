UNION ALL 将两个流给合并起来，要求两个流的字段完全一致，包括字段类型、字段顺序。

特别注意的是: Dipper中的UNION和UNION ALL 语义是一样的，在blink中UNION 是不允许重复值。如果需要去重，请用distinct。

## 语法格式

```sql
select_statement
UNION
ALL
select_statement;
```

## 示例

```sql
create table test_source_union1
(
    a varchar,
    b bigint,
    c bigint
) WITH (
      type = 'datahub',
      endpoint = '',
      accessId = '',
      accessKey = '',
      projectName = '',
      topic = '',
      project = ''
      );

create table test_source_union2
(
    a varchar,
    b bigint,
    c bigint
) WITH (
      type = 'datahub',
      endpoint = '',
      accessId = '',
      accessKey = '',
      projectName = '',
      topic = '',
      project = ''
      );

create table test_source_union3
(
    a varchar,
    b bigint,
    c bigint
) WITH (
      type = 'datahub',
      endpoint = '',
      accessId = '',
      accessKey = '',
      projectName = '',
      topic = '',
      project = ''
      );

create table test_result_union
(
    d VARCHAR,
    e BIGINT,
    f BIGINT,
    primary key (d)
) WITH (
      type = 'rds',
      url = '',
      username = '',
      password = '',
      tableName = ''
      );
INSERT into test_result_union

SELECT a,
       sum(b),
       sum(c)
FROM (SELECT *
      from test_source_union1
      UNION ALL
      SELECT *
      from test_source_union2
      UNION ALL
      SELECT *
      from test_source_union3
     ) t
GROUP BY a;
```
