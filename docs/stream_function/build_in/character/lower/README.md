# 语法

```sql
LOWER(A)
```

# 入参

- A VARCHAR 类型。

# 功能描述

返回转换为小写字符的字符串。

# 示例

- 测试数据

| var1(VARCHAR) | 
|---------------| 
| Ss            | 
| yyT           |

- 测试案例

```sql
SELECT LOWER(var1) as aa
FROM T1;
```

- 测试结果

| aa(VARCHAR) |
|-------------|
| ss          |
| yyt         |

