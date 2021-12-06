## 语法

```sql
VARCHAR MD5(VARCHAR str)
```

## 入参

- str VARCHAR 类型

## 功能描述

返回字符串的 MD5 值。

* 如果参数为 NULL 返回 NULL；
* (From 2.2.0) 如果参数为空串返回 d41d8cd98f00b204e9800998ecf8427e。

## 示例

- 测试数据 | str1(VARCHAR) | str2(VARCHAR) | | --- | --- | | k1=v1;k2=v2 | null |


- 测试案例

```sql
SELECT MD5(str1) as var1,
       MD5(str2) as var2
FROM T1
```

- 测试结果

  | var1(VARCHAR) | var2(VARCHAR) |
    | --- | --- |
  | 19c17f42b4d6a90f7f9ffc2ea9bdd775 | null |

