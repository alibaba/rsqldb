当`DISTINCT`用在`SELECT Clause`中表示对查询的columns进行去重，这里是对单实例进行去重。适配的场景是通过去重大幅减少数据量，这个语义并不能保证全局去重，如果依赖全局去重的语义，请使用groupby。 ​

比如：现在启动了10个Dipper实例，每个实例独立去重，对于相同的去重key，同一时刻全局会有10条输出，并非1条。

# 语法

```sql
SELECT DISTINCT expressions
FROM tables
         ...
```

- DISTINCT - 必须放到开始位置。（和其他函数一起使用时也是一样的，例如concat_agg(DISTINCT ',', device_id)）
- expressions - 是N(N>=1)个expression，可以是具体的column，也可以是function等任何合法表达式

# 示例

## SQL 语句

我们以如下Dipper SQL例直观感知一下`DISTINCT`的语义，如下：

```sql
CREATE TABLE distinct_tab_source
(
    FirstName VARCHAR,
    LastName  VARCHAR
) WITH (
      type = 'random'
      );
CREATE TABLE distinct_tab_sink
(
    FirstName VARCHAR,
    LastName  VARCHAR
) WITH (
      type = 'print'
      );
INSERT INTO distinct_tab_sink
SELECT DISTINCT FirstName, LastName -- 以 FirstName和LastName 两个列进行去重
FROM distinct_tab_source;
```

- 非精确去重，在每个并发做去重，数据不用shuffle，可以大大提高去重性能
- 用高压内存在内存存储累计数据，千万数据只占330兆内存，内存开销非常小
- 去重的周期有两个条件：1.时间，超过1个小时（可配置），会重新去重；2.即使不到1个小时，当内存的数据条数超过300w，会重新去重。
- 优点：基本上无资源开销，无shuffle开销，性能最大化
- 缺点：非全局去重，每个并发独立去重，比如有100个并发，去重后下游数据会收到100条数据，适合非精确去重
- **注：如果需要精确的全局去重，请使用groupby**

```sql
SELECT expressions
FROM tables
GROUP BY expressions;
```

例如，会对groupby 的健进行shuffle，全局相同的groupby字段（FirstName, LastName）只有一条输出

```sql
CREATE TABLE distinct_tab_source
(
    FirstName VARCHAR,
    LastName  VARCHAR
) WITH (
      type = 'random'
      );
CREATE TABLE distinct_tab_sink
(
    FirstName VARCHAR,
    LastName  VARCHAR
) WITH (
      type = 'print'
      );
CREATE TABLE distinct_tab_sink2
(
    FirstName VARCHAR,
    LastName  VARCHAR
) WITH (
      type = 'print'
      );
INSERT INTO distinct_tab_sink2
SELECT FirstName, LastName
FROM distinct_tab_source
GROUP BY FirstName, LastName; -- 以 FirstName和LastName 两个列进行去重
```

# DISTINCT in COUNT AGG

DISTINCT 在COUNT AGG中使用是统计去重后的计数。目前这块还不能支持容错的能力，系统挂掉，重新启动会导致结果不准确，还在完善中

## 语法

```sql
COUNT(DISTINCT expression)
```

## SQL语句

```sql
CREATE TABLE distinct_tab_source
(
    FirstName VARCHAR,
    LastName  VARCHAR
) WITH (
      type = 'random'
      );
CREATE TABLE distinct_tab_sink
(
    cnt          BIGINT,
    distinct_cnt BIGINT
) WITH (
      type = 'print'
      );
INSERT INTO distinct_tab_sink
SELECT COUNT(FirstName),        -- 不去重
       COUNT(DISTINCT FirstName)-- 按 FirstName 去重
FROM distinct_tab_source;
```
