# 语法

```sql
BIGINT
YEAR(TIMESTAMP time)
BIGINT YEAR(DATE date)
```

# 入参

- date DATE 类型日期。
- time TIMESTAMP 类型日期。

# 功能描述

返回传入时间的年份。

# 示例

- 测试数据

| tsStr(VARCHAR) | dateStr(VARCHAR) | tdate(DATE) | ts(TIMESTAMP) |
| --- | --- | --- | --- |
| 2017-10-15 00:00:00 | 2017-09-15 | 2017-11-10 | 2017-10-15 00:00:00 |

- 测试案例

```sql
SELECT YEAR (TIMESTAMP '2016-09-15 00:00:00') as int1,
    YEAR (DATE '2017-09-22') as int2,
    YEAR (tdate) as int3,
    YEAR (ts) as int4,
    YEAR (CAST (dateStr AS DATE)) as int5,
    YEAR (CAST (tsStr AS TIMESTAMP)) as int6
FROM T1
```

- 测试结果

| int1(BIGINT) | int2(BIGINT) | int3(BIGINT) | int4(BIGINT) | int5(BIGINT) | int6(BIGINT) |
| --- | --- | --- | --- | --- | --- |
| 2016 | 2017 | 2017 | 2017 | 2015 | 2017 |

