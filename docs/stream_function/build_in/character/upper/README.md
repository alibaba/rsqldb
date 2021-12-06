## 语法

```sql
VARCHAR UPPER(A)
```

## 入参

| 参数 | 数据类型 |
| --- | --- |
| A | VARCHAR |

## 功能描述

返回转换为大写字符的字符串。

## 示例

- 测试数据 | var1(VARCHAR) | | --- | | ss | | ttee |

- 测试语句

```
SELECT UPPER(var1) as aa
FROM T1;
```

- 测试结果

  | aa(VARCHAR) |
    | --- |
  | SS |
  | TTEE |

