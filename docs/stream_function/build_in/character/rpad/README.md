# 语法

```sql
VARCHAR RPAD(VARCHAR str, INT len, VARCHAR pad)
```

# 入参

- str VARCHAR 类型，启始的字符串。
- len INT 类型，新的字符串的长度。
- pad VARCHAR 类型，需要重复补充的字符串。

# 功能描述

字符串str右端填充若干个字符串pad, 直到新的字符串达到指定长度len为止 任意参数为null时返回null，主字符串为空串或目标长度为负数时为null，pad为空串时，若len不大于主字符串长度，返回主字符串裁剪后的结果，若len大于主字符串长度，返回null。

# 示例

- 测试数据

| str(VARCHAR) | len(INT) | pad(VARCHAR) | 
| --- | --- | --- | 
| "" | -2 | "" | 
| HelloWorld | 15 | John | 
| John | 2 | C | 
| C | 4 | HelloWorld | 
| null | 2 | C | 
| c | 2 | null | 
| asd | 2 | “” | 
| "" | 2 | s | 
| asd | 4 | "" | 
| "" | 0 | "" |

- 测试案例

```sql
SELECT RPAD(str, len, pad) as result
FROM T1
```

- 测试结果

| result(VARCHAR) |
| --- |
| null |
| HelloWorldJohnJ |
| Jo |
| CHel |
| null |
| null |
| as |
| ss |
| null |
| "" |

