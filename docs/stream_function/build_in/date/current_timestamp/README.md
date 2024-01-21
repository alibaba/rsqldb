# 语法

```sql
TIMESTAMP CURRENT_TIMESTAMP
```

# 入参

无

# 功能描述

返回当前时间的时间戳。内部表示为格林威治时间1970年01月01日00时00分00秒(北京时间1970年01月01日08时00分00秒)
起至现在的总毫秒数。与LOCALTIMESTAMP语义相同。

# 示例

- 测试案例

```sql
SELECT CURRENT_TIMESTAMP as var1
FROM T1
```

- 测试结果

| var1(TIMESTAMP)         |
|-------------------------|
| 2007-04-30 13:10:02.047 |

