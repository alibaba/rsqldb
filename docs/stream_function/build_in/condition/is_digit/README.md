## 语法

```sql
BOOLEAN
IS_DIGIT(VARCHAR str)
```

## 入参

- str VARCHAR 类型，指定的字符串。

## 功能描述

str中只包含数字则返回TRUE，否则返回FALSE，返回值为BOOLEAN类型。

## 示例

- 测试数据

  | e(VARCHAR) | f(VARCHAR) | g(VARCHAR) |
    | --- | --- | --- |
  | 3 | asd | null |

- 测试案例

```sql
SELECT IS_DIGIT(e) as boo1, IS_DIGIT(f) as boo2, IS_DIGIT(g) as boo3
FROM T1
```

- 测试结果

  | boo1(BOOLEAN) | boo2(BOOLEAN) | boo3(BOOLEAN) |
    | --- | --- | --- |
  | true | false | false |

