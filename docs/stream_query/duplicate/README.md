去重是用户经常遇到的需求，因为原数据中可能会因为客观原因存在一些重复数据。一般来说，去重的目标是降低数量，而非真的去重到一条，所以提供了两种实现：高性能版本和精确去重版本

注：ORDER BY后的时间属性字段必须在源表中定义

# 语法

由于 SQL 上没有直接支持去重的语法，因此我们使用了 SQL 的 ROW_NUMBER OVER WINDOW 功能来实现去重语法。这非常像我们 TopN
支持的方案。实际上去重就是一种特殊的 TopN。

```sql
SELECT *
FROM (SELECT *,
             ROW_NUMBER() OVER ([PARTITION BY col1[, col2..]
     ORDER BY timeAttributeCol [asc|desc]) AS rownum
      FROM table_name)
WHERE rownum = 1
```

- `ROW_NUMBER()` : 是一个计算行号的OVER窗口函数，行号计算从1开始。
- `PARTITION BY col1[, col2..]`  ： 指定分区的列，可以不指定。即去重的 keys。
- `ORDER BY timeAttributeCol [asc|desc])` :
  指定排序的列，必须是一个[时间属性](https://yuque.antfin-inc.com/rtcompute/doc/sql-time-attribute)的字段（即 proctime 或
  rowtime）。可以指定顺序（即 keep first row）OR倒序 (即 keep last row)。
- 外层查询 rownum 必须要 `= 1` 或者 `<= 1` 。条件必须是 `AND` 条件，并且不能有 Undeterministic 的 UDF 的条件。

如上语法所示，去重需要两层query，子查询中使用 ROW_NUMBER()
窗口函数来对数据根据时间属性列进行排序并标上排名。外层查询中，对排名进行过滤，只取第一条，达到了去重的目的。排序方向可以是按照时间列顺序也可以倒序，顺序并取第一条也就是
deduplicate keep first row。 last row目前不支持~~倒序并取第一条也就是 deduplicate keep last row~~。

# 实现说明

- 非精确去重，在每个并发做去重，数据不用shuffle，可以大大提高去重性能
- 用高压内存在内存存储不重复的分区值，千万数据只占330兆内存，内存开销非常小
- 去重的周期有两个条件：1.时间，超过1个小时，会重新编号；2.即使不到1个小时，当内存的数据条数超过300w，会重新编号。
- 优点：基本上无资源开销，无shuffle开销，性能最大化
- 缺点：非全局去重，每个并发独立去重，比如有100个并发，去重后下游数据会收到100条数据，适合非精确去重



