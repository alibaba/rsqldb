# 语法

```sql
 INITCAP
(A)
```

# 入参

- A VARCHAR 类型。

# 功能描述

返回字符串，每个字转换器的第一个字母大写，其余为小写。

# 示例

- 测试数据

| var1(VARCHAR) |
|---------------| 
| aADvbn        |

- 测试案例

```sql
SELECT INITCAP(var1) as aa
FROM T1;
```

- 测试结果

| aa(VARCHAR) |
| --- |
| Aadvbn |

