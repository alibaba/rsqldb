## 语法

```sql
VARCHAR CHR(INT ascii)
```

## 入参

- ascii INT 类型，是0到255之间的整数。如果不在此范围内，则返回NULL。

## 功能描述

将ASCII码转换为字符.

## 示例

- 测试数据 | int1(INT) | int2(INT) | int3(INT) | | --- | --- | --- | | 255 | 97 | 65 |


- 测试案例

```sql
SELECT CHR(int1) as var1, CHR(int2) as var2, CHR(int3) as var3
FROM T1
```

- 测试结果

  | var1(VARCHAR) | var2(VARCHAR) | var3(VARCHAR) |
      | --- | --- | --- |
  | ÿ | a | A |

