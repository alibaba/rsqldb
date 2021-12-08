如果计算的逻辑比较复杂用一个StreamSQL难以描述，Dipper支持通过定义视图的方式来简化开发的过程。需要明确的是，**视图仅仅用于辅助计算逻辑的描述，不会产生数据的物理存储**。视图是一种方便的方法，可以通过提供这些片段名称将SQL转换成可管理的视图块。因此，视图并不占用系统空间。

# 语法：

```sql
CREATE VIEW viewName
    [ (columnName[ , columnName]*) ]
AS queryStatement;
```

# 示例一

```sql

CREATE VIEW LargeOrders(r, t, c, u) AS
SELECT rowtime,
       productId,
       c,
       units
FROM Orders;

INSERT INTO rds_output
SELECT r,
       t,
       c,
       u
FROM LargeOrders;
```

# 案例二：

测试数据

| a(varchar) | b(bigint) | c(TIMESTAMP) |
| --- | --- | --- |
| test1 | 1 | 1506823820000 |
| test2 | 1 | 1506823850000 |
| test1 | 1 | 1506823810000 |
| test2 | 1 | 1506823840000 |
| test2 | 1 | 1506823870000 |
| test1 | 1 | 1506823830000 |
| test2 | 1 | 1506823860000 |

# SQL案例

```sql
CREATE TABLE datahub_stream
(
    a varchar,
    b BIGINT,
    c TIMESTAMP,
    d AS PROCTIME()
) WITH (
      type = 'datahub',
      endPoint = '',
      project = '',
      topic = '',
      accessId = '',
      accessKey = ''
      );
CREATE TABLE rds_output
(
    a   varchar,
    b   TIMESTAMP,
    cnt BIGINT,
    PRIMARY KEY (a)
) with (
      type = 'rds',
      url = '',
      tableName = '',
      userName = '',
      password = ''
      );
CREATE VIEW rds_view AS
SELECT a,
       CAST(HOP_START(d, interval '5' second, interval '30' second) AS TIMESTAMP) AS cc,
       sum(b)                                                                     AS cnt
FROM datahub_stream
GROUP BY HOP(d, interval '5' second, interval '30' second), a;

INSERT INTO rds_output
SELECT a,
       cc,
       cnt
FROM rds_view
WHERE cnt = 4
```

# 测试结果

| a(varchar) | b (TIMESTAMP) | cnt (BIGINT) |
| --- | --- | --- |
| test2 | 2017-11-06 16:54:10 | 4 |

