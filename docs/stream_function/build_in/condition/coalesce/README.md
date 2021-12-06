## 语法

```sql
COALESCE(A,B,...)
```

## 入参

- A,B 任意数据类型

## 功能描述

返回列表中第一个非null的值，返回值类型和参数类型相同。如果列表中所有的值都是null，则返回null。

## 示例

- 测试数据 | var1 (VARCHAR) | var2 (VARCHAR) | | --- | --- | | null | 30 |


- 测试案例

```sql
SELECT COALESCE(var1, var2) as aa
FROM T1
```

- 测试结果

  | aa (VARCHAR) |
    | --- |
  | 30 |

