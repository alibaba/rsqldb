# 语法

```sql
TIMESTAMP TO_TIMESTAMP(BIGINT time)
TIMESTAMP TO_TIMESTAMP(VARCHAR date)
TIMESTAMP TO_TIMESTAMP(VARCHAR date, VARCHAR format)
```

# 功能描述

将bigint类型的日期或者varchar类型的日期转换成TimeStamp类型。

# 参数说明：

- time BIGINT 类型，单位为毫秒。语义为格林威治时间1970年01月01日00时00分00秒(北京时间1970年01月01日08时00分00秒)起至某时刻的总毫秒数。
- date VARCHAR 类型，默认格式为 yyyy-MM-dd HH:mm:ss[.SSS]。
- format VARCHAR 类型，指定输入日期格式。  (注: 因为在SQL中整个 pattern 使用单引号进行界定，当 pattern 中包含字符常量时, 如'T', 需要使用**两个单引号**进行转义 ''T'')

| format 示例 | timestamp |
| --- | --- |
| yyyy-MM-dd HH:mm:ss | 2017-09-15 00:00:00 |
| yyyyMMddHHmmss | 20170915000000 |
| yyyy-MM-dd'T'HH:mm:ss.SSSXXX | 2019-05-06T10:59:16.000+08:00 |

# 示例

- 测试数据

| timestamp1(bigint) | timestamp2(VARCHAR) | timestamp3(VARCHAR) | timestamp4(VARCHAR) |
| --- | --- | --- | --- |
| 1513135677000 | 2017-09-15 00:00:00 | 20170915000000 | 2019-05-06T10:59:16.000+08:00 |

- 测试案例

```sql
SELECT TO_TIMESTAMP(timestamp1)                   as var1,
       TO_TIMESTAMP(timestamp2)                   as var2,
       TO_TIMESTAMP(timestamp3, 'yyyyMMddHHmmss') as var3
    TO_TIMESTAMP(timestamp4, 'yyyy-MM-dd''T''HH:mm:ss.SSSXXX') as var3
FROM T1
```

- 测试结果

| var1(TIMESTAMP) | var2(TIMESTAMP) | var3(TIMESTAMP) | var4(TIMESTAMP) |
| --- | --- | --- | --- |
| 2017-12-13 03:27:57.0 | 2017-09-15 00:00:00.0 | 2017-09-15 00:00:00.0 | 2019-05-06 10:59:16.000 |

