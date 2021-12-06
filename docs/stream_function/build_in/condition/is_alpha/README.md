## 语法

```sql
BOOLEAN
IS_ALPHA(VARCHAR str)
```

## 入参

- str VARCHAR 类型，指定的字符串。

## 功能描述

str中只包含字母则返回TRUE，否则返回FALSE。

## 示例

- 测试数据

  | e(VARCHAR) | f(VARCHAR) | g(VARCHAR) |
    | --- | --- | --- |
  | 3 | asd | null |

- 测试案例

```sql
SELECT IS_ALPHA(e) as boo1, IS_ALPHA(f) as boo2, IS_ALPHA(g) as boo3
FROM T1
```

- 测试结果

  | boo1(BOOLEAN) | boo2(BOOLEAN) | boo3(BOOLEAN) |
    | --- | --- | --- |
  | false | true | false |

