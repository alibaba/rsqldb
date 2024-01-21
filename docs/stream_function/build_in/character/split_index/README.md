# 语法

```sql
VARCHAR SPLIT_INDEX(VARCHAR str, VARCHAR sep, INT index)
```

# 入参

- str VARCHAR 类型，被分隔的字符串。
- sep VARCHAR 类型，以什么为分隔符的字符串。
- index INT 类型 获取的第几段参数值。

# 功能描述

以sep作为分隔符，将字符串str分隔成若干段，取其中的第index段，取不到返回NULL，index从0开始 任一参数为NULL，返回NULL。

# 示例

- 测试数据

| str(VARCHAR)   | sep(VARCHAR) | index(INT) | 
|----------------|--------------|------------| 
| Jack,John,Mary | ,            | 2          | 
| Jack,John,Mary | ,            | 3          | 
| Jack,John,Mary | null         | 0          | 
| null           | ,            | 0          |

- 测试案例

```sql
SELECT SPLIT_INDEX(str, sep, index) as var1
FROM T1
```

- 测试结果

| var1(VARCHAR) |
|---------------|
| Mary          |
| null          |
| null          |
| null          |

