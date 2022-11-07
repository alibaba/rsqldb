# 语法

```sql
VARCHAR SUBSTRING(VARCHAR a, INT start)
VARCHAR SUBSTRING(VARCHAR a, INT start, INT len)
```

# 入参

- a VARCHAR 类型，指定的字符串。
- len INT 类型，截取的长度。
- start INT 类型，截取从字符串a开始的位置。

# 功能描述

获取字符串子串，截取从位置start开始长度为len的子串，若未指定len则截取到字符串结尾.start 从1开始，start为零当1看待,为负数时表示从字符串末尾计算位置。

# 示例

- 测试数据

| str(VARCHAR) | nullstr(VARCHAR) | 
| --- | --- | 
| k1=v1;k2=v2 | null |

- 测试案例

```sql
SELECT SUBSTRING('', 222222222) as var1,
       SUBSTRING(str, 2)        as var2,
       SUBSTRING(str, -2)       as var3,
       SUBSTRING(str, -2, 1)    as var4,
       SUBSTRING(str, 2, 1)     as var5,
       SUBSTRING(str, 22)       as var6,
       SUBSTRING(str, -22)      as var7,
       SUBSTRING(str, 1)        as var8,
       SUBSTRING(str, 0)        as var9,
       SUBSTRING(nullstr, 0)    as var10
FROM T1
```

- 测试结果

| var1(VARCHAR) | var2(VARCHAR) | var3(VARCHAR) | var4(VARCHAR) | var5(VARCHAR) | var6(VARCHAR) | var7(VARCHAR) | var8(VARCHAR) | var9(VARCHAR) | var10(VARCHAR) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| "" | 1=v1;k2=v2 | v2 | v | 1 | "" | "" | k1=v1;k2=v2 | k1=v1;k2=v2 | null |

