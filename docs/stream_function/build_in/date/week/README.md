# 语法

```sql
BIGINT
WEEK(DATE date)
BIGINT WEEK(TIMESTAMP time)
```

# 入参

- date DATE类型日期。
- time TIMESTAMP类型，时间戳格式的日期。

# 功能描述

计算指定日期在一年中的第几周，周数取值区间 1~53。

# 示例

- 测试数据

| dateStr(VARCHAR) | date1(DATE) | ts1(TIMESTAMP)      |
|------------------|-------------|---------------------|
| 2017-09-15       | 2017-11-10  | 2017-10-15 00:00:00 |

- 测试案例

```sql
SELECT WEEK(TIMESTAMP '2017-09-15 00:00:00') as int1,
       WEEK(date1)                           as int2,
       WEEK(ts1)                             as int3,
       WEEK(CAST(dateStr AS DATE))           as int4
FROM T1
```

- 测试结果

| int1(BIGINT) | int2(BIGINT) | int3(BIGINT) | int4(BIGINT) |
|--------------|--------------|--------------|--------------|
| 37           | 45           | 41           | 37           |


