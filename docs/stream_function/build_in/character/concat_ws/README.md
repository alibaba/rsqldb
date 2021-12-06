## 语法

```sql
VARCHAR CONCAT_WS(VARCHAR separator, VARCHAR var1, VARCHAR var2, ...)
```

## 入参

- separator VARCHAR 类型，是指的是分隔符号。
- var1 VARCHAR 类型的参数值。
- var2 VARCHAR 类型的参数值。

## 功能描述

将每个参数值和第一个参数separator指定的分隔符依次连接到一起组成新的字符串,长度和类型取决于输入值。

注意: 当separator取值为null，则将separator视作空串进行拼接；当其它参数为NULL，在执行拼接过程中跳过该为NULL的参数。

## 示例

- 测试数据 | sep(VARCHAR) | str1(VARCHAR) | str2(VARCHAR) | str3(VARCHAR) | | --- | --- | --- | --- | | | | Jack | Harry | John | | null | Jack | Harry | John | | | | null | Harry | John | | | | Jack | null | null |


- 测试案例

```sql
SELECT CONCAT_WS(sep, str1, str2, str3) as var
FROM T1
```

- 测试结果

  | var(VARCHAR) |
    | --- |
  | Jack|Harry|John |
  | JackHarryJohn |
  | Harry|John |
  | Jack |

