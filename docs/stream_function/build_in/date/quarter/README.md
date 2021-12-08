# 语法

```sql
BIGINT
QUARTER(TIMESTAMP time)
BIGINT QUARTER(DATE date)
```

# 入参

- date DATE 类型日期。
- time TIMESTAMP 类型日期。

# 功能描述

返回传入时间是第几个季节。

# 示例

- 测试数据

| tdate(DATE) | ts(TIMESTAMP) |
| --- | --- |
| 2016-04-12 | 2016-04-12 00:00:00 |

- 测试案例

```sql
SELECT QUARTER(tdate),
       QUARTER(ts)
FROM T1
```

- 测试结果

| int1(BIGINT) | int2(BIGINT) |
| --- | --- |
| 2 | 2 |

