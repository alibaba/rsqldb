# 语法

```sql
Date TO_DATE(INT time)
Date TO_DATE(VARCHAR date)
Date TO_DATE(VARCHAR date, VARCHAR format)
```

# 功能描述

将int类型的日期或者varchar类型的日期转换成Date类型。

# 参数说明：

- time
    - INT 类型，单位为天，表示从1970-1-1到所表示时间的天数。
- date
    - VARCHAR 类型，默认格式为 yyyy-MM-dd
- format
    - VARCHAR 类型，指定输入日期格式。

# 示例

- 测试数据

| date1(INT) | date2(VARCHAR) | date3(VARCHAR) |
|------------|----------------|----------------|
| 100        | 2017-09-15     | 20170915       |

# 测试案例

```sql
SELECT TO_DATE(date1)             as var1,
       TO_DATE(date2)             as var2,
       TO_DATE(date3, 'yyyyMMdd') as var3
FROM T1
```

- 测试结果

| var1(Date) | var2(Date) | var3(Date) |
|------------|------------|------------|
| 1970-04-11 | 2017-09-15 | 2017-09-15 |

