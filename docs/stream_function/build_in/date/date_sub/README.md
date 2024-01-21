# 语法

```sql
VARCHAR DATE_SUB(VARCHAR startdate, INT days)
VARCHAR DATE_SUB(TIMESTAMP time, INT days)
```

# 入参

- startdate VARCHAR类型日期，日期格式：yyyy-MM-dd 或 yyyy-MM-dd HH:mm:ss。
- time TIMESTAMP类型日期。
- days INT类型，指定偏移的天数。

# 功能描述

时间相减，为日期减去天数，日期格式可以是yyyy-MM-dd hh:mm:
ss或yyyy-MM-dd或TIMESTAMP，返回VARCHAR格式的日期yyyy-MM-dd，若有参数为null或解析错误，返回null。

# 示例

- 测试数据

| date1(VARCHAR) | nullstr(VARCHAR) |
|----------------|------------------|
| 2017-10-15     | null             |

- 测试案例

```sql
SELECT DATE_SUB(date1, 30)                           as var1,
       DATE_SUB(TIMESTAMP '2017-10-15 23:00:00', 30) as var2,
       DATE_SUB(nullstr, 30)                         as var3
FROM T1
```

- 测试结果

| var1(VARCHAR) | var2(VARCHAR) | var3(VARCHAR) |
|---------------|---------------|---------------|
| 2017-09-15    | 2017-09-15    | null          |


