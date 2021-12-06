## 语法

```sql
 BIGINT MONTH(TIMESTAMP time)
 BIGINT MONTH(DATE date)
```

## 入参

- date DATE 类型日期。
- time TIMESTAMP 类型日期。

## 功能描述

返回输入时间参数中的月份，范围1～12。

## 示例

- 测试数据 | tsStr(VARCHAR) | dateStr(VARCHAR) | tdate(DATE) | ts(TIMESTAMP) | | --- | --- | --- | --- | | 2017-10-15 00:00:00 | 2017-09-15 | 2017-11-10 | 2017-10-15 00:00:00 |


- 测试案例

```sql
SELECT MONTH (TIMESTAMP '2016-09-15 00:00:00') as int1,
    MONTH (DATE '2017-09-22') as int2,
    MONTH (tdate) as int3,
    MONTH (ts) as int4,
    MONTH (CAST (dateStr AS DATE)) as int5,
    MONTH (CAST (tsStr AS TIMESTAMP)) as int6
FROM T1
```

- 测试结果

  | int1(BIGINT) | int2(BIGINT) | int3(BIGINT) | int4(BIGINT) | int5(BIGINT) | int6(BIGINT) |
    | --- | --- | --- | --- | --- | --- |
  | 9 | 9 | 11 | 10 | 9 | 10 |

