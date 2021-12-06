## 语法

```sql
VARCHAR TO_BASE64(bin)
```

## 入参

- bin BINARY类型

## 功能描述

将BINARY类型数据转换成对应base64编码的字符串输出。

## 示例

- 测试数据 | c(VARCHAR) | | --- | | SGVsbG8gd29ybGQ= | | SGk= | | SGVsbG8= |


- 测试案例

```sql
SELECT TO_BASE64(FROM_BASE64(c)) as var1
FROM T1
```

- 测试结果

  | var1(VARCHAR) |
    | --- |
  | SGVsbG8gd29ybGQ= |
  | SGk= |
  | SGVsbG8= |

