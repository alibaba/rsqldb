## 语法

```sql
INT HASH_CODE(VARCHAR str)
```

## 入参

- str VARCHAR 类型

## 功能描述

返回字符串的hashCode()的绝对值。

## 示例

- 测试数据 | str1(VARCHAR) | str2(VARCHAR) | nullstr(VARCHAR) | | --- | --- | --- | | k1=v1;k2=v2 | k1:v1,k2:v2 | null |


- 测试案例

```sql
SELECT HASH_CODE(str1) as var1, HASH_CODE(str2) as var2, HASH_CODE(nullstr) as var3
FROM T1
```

- 测试结果

  | var1(INT) | var2(INT) | var3(INT) |
    | --- | --- | --- |
  | 1099348823 | 401392878 | null |

