# 语法

```sql
CHAR_LENGTH(A)
```

# 入参

- A VARCHAR 类型。

# 功能描述

返回字符串中的字符数。

# 示例

- 测试数据

| var1(VARCHAR) | 
| --- | 
| ss | 
| 231ee |

- 测试案例

```sql
SELECT CHAR_LENGTH(var1) as aa
FROM T1;
```

- 测试结果

| aa(INT) | 
| --- | 
| 2 | 
| 5 |

