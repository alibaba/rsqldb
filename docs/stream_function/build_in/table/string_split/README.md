## 语法

```sql
STRING_SPLIT
(VARCHAR , separator)
```

## 入参

- separator VARCHAR 类型,指定的分隔符的字符串。

## 功能描述

将字符串按照splitchar为分隔符分割成多个部分。

## 示例

- 测试数据

  | d(VARCHAR) | s(VARCHAR) |
    | --- | --- |
  | abc-bcd | - |
  | hhh | - |


- 测试案例

```sql
SELECT d, v
FROM T1,
     lateral table(STRING_SPLIT(d, s)) 
as T(v)
```

- 测试结果

  | d(VARCHAR) | v(VARCHAR) |
    | --- | --- |
  | abc-bcd | abc |
  | abc-bcd | bcd |
  | hhh | hhh |

##  
