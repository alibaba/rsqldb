# 语法

```sql
VARCHAR DATE_ADD(VARCHAR startdate, INT days)
VARCHAR DATE_ADD(TIMESTAMP time, INT days)
```

# 入参

- startdate VARCHAR类型日期，日期格式：yyyy-MM-dd 或 yyyy-MM-dd HH:mm:ss
- time TIMESTAMP类型日期。
- days INT类型，间隔的天数。

# 功能描述

返回指定startdate日期间隔后days天数的一个全新的VARCHAR类型日期，日期格式可以是yyyy-MM-dd hh:mm:
ss或yyyy-MM-dd或timestamp，返回string格式的日期yyyy-MM-dd，若有参数为null或解析错误，返回null。

# 示例

- 测试数据

| datetime1(VATCHAR)  | nullstr(VATCHAR) |
|---------------------|------------------|
| 2017-09-15 00:00:00 | null             |

- 测试案例

```sql
SELECT DATE_ADD(datetime1, 30)                       as var1,
       DATE_ADD(TIMESTAMP '2017-09-15 23:00:00', 30) as var2,
       DATE_ADD(nullstr, 30)                         as var3
FROM T1
```

- 测试结果

| var1(VARCHAR) | var2(VARCHAR) | var3(VARCHAR) |
|---------------|---------------|---------------|
| 2017-10-15    | 2017-10-15    | null          |

