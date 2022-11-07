# 语法

```sql
BIGINT
UNIX_TIMESTAMP()
BIGINT UNIX_TIMESTAMP(TIMESTAMP timestamp)
BIGINT UNIX_TIMESTAMP(VARCHAR date)
BIGINT UNIX_TIMESTAMP(VARCHAR date, VARCHAR format)
```

# 入参

- timestamp TIMESTAMP类型
- date VARCHAR类型日期，默认日期格式：yyyy-MM-dd HH:mm:ss。
- format VARCHAR类型, 指定输入参数日期格式

# 功能描述

两个参数，均为可选，无参数时返回当前时间的时间戳，单位为**秒**，与NOW语义相同。

- 第一个参数是字符串类型的时间
- 第二个参数是时间的格式，默认为yyyy-MM-dd HH:mm:ss
- 返回值是第一个参数转换成的长整型的时间戳，单位为**秒**，若有参数为null或解析错误，返回null。

# 示例

- 测试数据

| nullstr(VARCHAR) |
| --- |
| null |

- 测试案例

```sql
SELECT UNIX_TIMESTAMP()        as big1,
       UNIX_TIMESTAMP(nullstr) as big2
FROM T1
```

- 测试结果

| big1(BIGINT) | big2(BIGINT) |
| --- | --- |
| 1403006911 | null |

