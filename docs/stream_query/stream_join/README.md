流上join和传统批上join的语义一致，都是两张表的join（table a join table b），不同的是流上table a和table
b是两张动态表，join的结果也会动态更新，保证最终结果和批处理的一致性。

# 语法格式：

```sql
tableReference
[, tableReference ]* | tableexpression
[ LEFT ] JOIN tableexpression [ joinCondition ];
```

- 支持等值连接和不等值连接
- 支持 INNER JOIN, LEFT JOIN,
- 不支持RIGHT JOIN, FULL JOIN, ANTI JOIN, SEMI JOIN

# 示例：

```sql
--创建Datahub table作为数据源引用
create table datahub_stream1
(
    a int,
    b int,
    c varchar
) WITH (
      type = 'datahub',
      endpoint = '',
      accessId = '',
      accessKey = '',
      projectName = '',
      topic = '',
      project = ''
      );
--创建Datahub表作为维表
create table datahub_stream2
(
    a int,
    b int,
    c varchar
) WITH (
      type = 'datahub',
      endpoint = '',
      accessId = '',
      accessKey = '',
      projectName = '',
      topic = '',
      project = ''
      );
--用rds做结果表
create table rds_output
(
    s1_c varchar,
    s2_c varchar
) with (
      type = 'rds',
      url = '',
      tableName = '',
      userName = '',
      password = ''
      );
insert into rds_output
select s1.c,
       s2.c
from datahub_stream1 AS s1
         join datahub_stream2 AS s2 on s1.a = s2.a
where s1.a = 0;
```
