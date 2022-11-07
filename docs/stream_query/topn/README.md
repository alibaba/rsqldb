TopN 常用于计算流数据中某个指标的最大/最小的前N个数据。Blink SQL 可以基于 OVER 窗口操作灵活地完成 TopN 的工作。

# 语法

```sql
SELECT *
FROM (
         SELECT *,
                ROW_NUMBER() OVER ([PARTITION BY col1[, col2..]
   ORDER BY col1 [asc|desc][, col2 [asc|desc]...]) AS rownum
         FROM table_name)
WHERE rownum <= N[
  AND conditions]
```

参数说明：

- ROW_NUMBER(): 是一个计算行号的OVER窗口函数，行号计算从1开始。
- PARTITION BY col1[, col2..] ： 指定分区的列，可以不指定。
- ORDER BY col1 [asc|desc][, col2 [asc|desc]...]: 指定排序的列，可以多列不同排序方向。

如上语法所示，TopN 需要两层query，子查询中使用 ROW_NUMBER() 窗口函数来对数据根据排序列进行排序并标上排名。外层查询中，对排名进行过滤，只取前N条，如N=10，那么就是取 Top 10 的数据。

在物理执行过程中，Dipper会对输入的数据流根据排序键进行排序，如果某个分区的前N条记录发生了改变，则会将改变的那几名数据以更新流的形式发给下游。**所以如果需要将TopN的数据输出到外部存储，那么接的结果表必须是一个带primary key的表。**

WHERE 条件的一些限制：

为了Dipper能识别出这是一个TopN的query，外层循环中必须要指定 rownum <= N的格式来指定前N条记录，不能 rownum - 5 <= N 这种将rownum至于某个表达式中。当然，WHERE条件中，可以额外带上其他条件，但是必须是以 AND 连接。 ​

# 示例

该示例来自优酷的真实业务（精简后），优酷上每个视频在分发时会产生大量流量，依据视频产生的流量我们可以分析出最热门的视频。如下，就是统计出每分钟流量最大的 top5 的视频。

```sql
--从SLS读取数据原始存储表
CREATE TABLE source_data
(
    vid           VARCHAR,   -- video id
    rowtime       Timestamp, -- 观看视频发生的时间
    response_size BIGINT,    -- 观看产生的流量
    WATERMARK FOR rowtime as withOffset(rowtime, 0)
) WITH (
      type = 'sls',
      ...
      );

--1分钟窗口统计vid带宽数
CREATE VIEW vew_data AS
SELECT vid,
       TUMBLE_START(rowtime, INTERVAL '1' MINUTE) AS start_time,
       SUM(response_size)                         AS rss
FROM source_data
GROUP BY vid, TUMBLE(rowtime, INTERVAL '1' MINUTE);

--rds存储表
CREATE TABLE hbase_data
(
    vid        VARCHAR,
    band_width BIGINT,
    start_time VARCHAR,
    row_num    INT,
    PRIMARY KEY (start_time, row_num)
) WITH (
      type = 'rds',
      ...
      );

-- 统计每分钟 top5 消耗流量的 vid，并输出
INSERT INTO hbase_data
SELECT vid, rss, start_time, rownum
FROM (
         SELECT vid,
                start_time,
                rss,
                ROW_NUMBER() OVER (PARTITION BY start_time ORDER BY rss DESC) as rownum
         FROM vew_data
     )
WHERE rownum <= 5;
```
