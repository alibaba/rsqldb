## 语法

```sql
INT DATEDIFF(VARCHAR enddate, VARCHAR startdate)
INT DATEDIFF(TIMESTAMP enddate, VARCHAR startdate)
INT DATEDIFF(VARCHAR enddate, TIMESTAMP startdate)
INT DATEDIFF(TIMESTAMP enddate, TIMESTAMP startdate)
```

## 入参

- startdate TIMESTAMP类型或VARCHAR类型日期，日期格式：yyyy-MM-dd 或 yyyy-MM-dd HH:mm:ss。
- enddate TIMESTAMP类型或VARCHAR类型日期，日期格式：yyyy-MM-dd 或 yyyy-MM-dd HH:mm:ss。

## 功能描述

计算从enddate到startdate两个时间的天数差值，日期格式可以是`yy-MM-dd HH:mm:ss`或`yy-MM-dd`或timestamp，返回整数，若有参数为null或解析错误，返回null。

## 示例

- 测试数据

  | datetime1(VARCHAR) | datetime2(VARCHAR) | nullstr(VARCHAR) |
    | --- | --- | --- |
  | 2017-10-15 00:00:00 | 2017-09-15 00:00:00 | null |


- 测试案例

```sql
SELECT DATEDIFF(datetime1, datetime2)                                            as int1,
       DATEDIFF(TIMESTAMP '2017-10-15 23:00:00', datetime2)                      as int2,
       DATEDIFF(datetime2, TIMESTAMP '2017-10-15 23:00:00')                      as int3,
       DATEDIFF(datetime2, nullstr)                                              as int4,
       DATEDIFF(nullstr, TIMESTAMP '2017-10-15 23:00:00')                        as int5,
       DATEDIFF(nullstr, datetime2)                                              as int6,
       DATEDIFF(TIMESTAMP '2017-10-15 23:00:00', TIMESTAMP '2017-9-15 00:00:00') as int7
FROM T1
```

- 测试结果

  | int1(INT) | int2(INT) | int3(INT) | int4(INT) | int5(INT) | int6(INT) | int7(INT) |
    | --- | --- | --- | --- | --- | --- | --- |
  | 30 | 30 | -30 | null | null | null | 30 |



