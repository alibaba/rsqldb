## 语法

```sql
VARCHAR REVERSE(VARCHAR str)
```

## 入参

- str VARCHAR 类型，普通字符串值。

## 功能描述

反转字符串，返回字符串值的相反顺序。任一参数为NULL，返回NULL。

## 示例

- 测试数据 | str1(VARCHAR) | str2(VARCHAR) | str3(VARCHAR) | str4(VARCHAR) | | --- | --- | --- | --- | | iPhoneX | Alibaba | World | null |


- 测试案例

```sql
SELECT REVERSE(str1) as var1,
       REVERSE(str2) as var2,
       REVERSE(str3) as var3,
       REVERSE(str4) as var4
FROM T1
```

- 测试结果

  | var1(VARCHAR) | var2(VARCHAR) | var3(VARCHAR) | var4(VARCHAR) |
      | --- | --- | --- | --- |
  | XenohPi | ababilA | dlroW | null |

