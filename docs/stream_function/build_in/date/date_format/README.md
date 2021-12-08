# 语法

```sql
VARCHAR DATE_FORMAT(TIMESTAMP time, VARCHAR to_format)
VARCHAR DATE_FORMAT(VARCHAR date, VARCHAR to_format)
VARCHAR DATE_FORMAT(VARCHAR date, VARCHAR from_format, VARCHAR to_format)
```

# 入参

- date VARCHAR类型日期，默认日期格式：yyyy-MM-dd HH:mm:ss。
- time TIMESTAMP类型的日期。
- from_format 指定输入日期格式
- to_format 指定输出日期格式

# 功能描述

将字符串类型的日期从源格式转换至目标格式，第一个参数为源字符串，第二个参数from_format可选，为源字符串的格式，默认为yyyy-MM-dd HH:mm:ss，第三个参数为返回日期的的格式，返回值为转换格式后的字符串类型日期，若有参数为null或解析错误，返回null。

# 示例

- 测试数据

| date1(VARCHAR) | datetime1(VARCHAR) | nullstr(VARCHAR) |
| --- | --- | --- |
| 0915-2017 | 2017-09-15 00:00:00 | null |

- 测试案例

```sql
SELECT DATE_FORMAT(datetime1, 'yyMMdd')                       as var1,
       DATE_FORMAT(nullstr, 'yyMMdd')                         as var2,
       DATE_FORMAT(datetime1, nullstr)                        as var3,
       DATE_FORMAT(date1, 'MMdd-yyyy', nullstr)               as var4,
       DATE_FORMAT(date1, 'MMdd-yyyy', 'yyyyMMdd')            as var5,
       DATE_FORMAT(TIMESTAMP '2017-09-15 23:00:00', 'yyMMdd') as var6
FROM T1
```

- 测试结果

| var1(VARCHAR) | var2(VARCHAR) | var3(VARCHAR) | var4(VARCHAR) | var5(VARCHAR) | var6(VARCHAR) |
| --- | --- | --- | --- | --- | --- |
| 170915 | null | null | null | 20170915 | 170915 |

