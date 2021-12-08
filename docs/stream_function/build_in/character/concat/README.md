# 语法

```sql
 VARCHAR concat(VARCHAR var1, VARCHAR var2, ...)
```

# 入参

- var1 VARCHAR 类型,普通字符串值。
- var2 VARCHAR 类型,普通字符串值。

# 功能描述

连接两个或多个字符串值从而组成一个新的字符串。任一参数为NULL，跳过该参数。特别注意：参数类型必须为varchar字段，不能是int/bigint等其他字段类型。

# 示例

- 测试数据

| var1(VARCHAR) | var2(VARCHAR) | var3(VARCHAR) | 
| --- | --- | --- | 
| Hello | My | World | 
| Hello | null | World | 
| null | null | World | 
| null | null | null |

- 测试案例

```sql
SELECT CONCAT(var1, var2, var3) as var
FROM T1
```

- 测试结果

| var(VARCHAR) |
| --- |
| HelloMyWorld |
| HelloWorld |
| World |
| “” |

