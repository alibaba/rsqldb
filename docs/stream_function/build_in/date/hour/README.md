## 语法

```sql
 BIGINT HOUR(TIMESTAMP timestamp)
 BIGINT HOUR(TIME time)
```

## 入参

- time TIME 类型时间。
- time TIMESTAMP 类型时间。

## 功能描述

返回输入时间参数中的24小时制的小时数，范围0～23。

## 示例

- 测试数据

  | datetime1(VARCHAR) | time1(VARCHAR) | time2(TIME) | timestamp1(TIMESTAMP) |
    | --- | --- | --- | --- |
  | 2017-10-15 11:12:13 | 22:23:24 | 22:23:24 | 2017-10-15 11:12:13 |


- 测试案例

```sql
SELECT HOUR (TIMESTAMP '2016-09-20 23:33:33') as int1,
    HOUR (TIME '23:30:33') as int2,
    HOUR (time2) as int3,
    HOUR (timestamp1) as int4,
    HOUR (CAST (time1 AS TIME)) as int5,
    HOUR (TO_TIMESTAMP(datetime1)) as int6
FROM T1
```

- 测试结果

  | int1(BIGINT) | int2(BIGINT) | int3(BIGINT) | int4(BIGINT) | int5(BIGINT) | int6(BIGINT) |
    | --- | --- | --- | --- | --- | --- |
  | 23 | 23 | 22 | 11 | 22 | 11 |

