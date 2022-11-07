# 语法

```sql
 BIGINT
DAYOFMONTH(TIMESTAMP time)
 BIGINT DAYOFMONTH(DATE date)
```

# 入参

- date DATE 类型日期。
- time TIMESTAMP 类型日期。

# 功能描述

返回输入时间参数中的日，范围1～31。

# 示例

- 测试数据

| tsStr(VARCHAR) | dateStr(VARCHAR) | tdate(DATE) | ts(TIMESTAMP) |
| --- | --- | --- | --- |
| 2017-10-15 00:00:00 | 2017-09-15 | 2017-11-10 | 2017-10-15 00:00:00 |

- 测试案例

```sql
SELECT DAYOFMONTH(TIMESTAMP '2016-09-15 00:00:00') as int1,
       DAYOFMONTH(DATE '2017-09-22')               as int2,
       DAYOFMONTH(tdate)                           as int3,
       DAYOFMONTH(ts)                              as int4,
       DAYOFMONTH(CAST(dateStr AS DATE))           as int5,
       DAYOFMONTH(CAST(tsStr AS TIMESTAMP))        as int6
FROM T1
```

- 测试结果

| int1(BIGINT) | int2(BIGINT) | int3(BIGINT) | int4(BIGINT) | int5(BIGINT) | int6(BIGINT) |
| --- | --- | --- | --- | --- | --- |
| 15 | 22 | 10 | 15 | 15 | 15 |

