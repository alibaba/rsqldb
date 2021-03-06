# 语法

```sql
BINARY
FROM_BASE64(str)
```

# 入参

- str VARCHAR 类型，base64编码的字符串。

# 功能描述

将base64编码的字符串str解析成对应的binary类型数据输出。

# 示例

- 测试数据

| a(INT) | b(BIGINT) | c(VARCHAR) | 
| --- | --- | --- | 
| 1 | 1L | null |

- 测试案例

```sql
SELECT from_base64(c)                  as var1,
       from_base64('SGVsbG8gd29ybGQ=') as var2
FROM T1
```

- 测试结果

| var1(BINARY) | var2(BINARY) |
| --- | --- |
| null | Byte Array: [72('H'), 101('e'), 108('l'), 108('l'), 111('o'), 32(' '), 119('w'), 111('o'), 114('r'), 108('l'), 100('d')] |

