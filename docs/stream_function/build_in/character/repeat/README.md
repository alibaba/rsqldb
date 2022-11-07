# 语法

```sql
VARCHAR REPEAT(VARCHAR str, INT n)
```

# 入参

- str VARCHAR 类型，重复字符串值。
- n INT 类型，重复次数。

# 功能描述

返回以str重复字符串值,重复次数为N的新的字符串。 参数为null返回null，重复次数为0或负数返回空串。

# 示例

- 测试数据

| str(VARCHAR) | n(INT) | 
| --- | --- | 
| J | 9 |
| Hello | 2 |
| Hello | -9 | 
| null | 9 |

- 测试案例

```sql
SELECT REPEAT(str, n) as var1
FROM T1
```

- 测试结果

| var1(VARCHAR) |
| --- |
| JJJJJJJJJ |
| HelloHello |
| "" |
| null |

