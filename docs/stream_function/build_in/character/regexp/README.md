## 语法

```sql
BOOLEAN
REGEXP(VARCHAR str, VARCHAR pattern)
```

## 入参

- str VARCHAR 类型，指定的字符串。
- pattern VARCHAR 类型，指定的匹配模式。

## 功能描述

指定str的字符串是否匹配指定的pattern进行正则匹配,str或者pattern为空或NULL返回false。

## 样例

- 测试数据 | str1(VARCHAR) | pattern1(VARCHAR) | | --- | --- | | k1=v1;k2=v2 | k2* | | k1:v1|k2:v2 | k3 | | null | k3 | | k1:v1|k2:v2 | null | | k1:v1|k2:v2 | ( |


- 测试案例

```sql
SELECT REGEXP(str1, pattern1) as result
FROM T1
```

- 测试结果

  | result(BOOLEAN) |
    | --- |
  | true |
  | false |
  | null |
  | null |
  | false |

