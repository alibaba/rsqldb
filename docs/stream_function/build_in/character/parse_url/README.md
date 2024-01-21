# 语法

```sql
VARCHAR PARSE_URL(VARCHAR urlStr, VARCHAR partToExtract [, VARCHAR key])
```

# 入参

- urlStr VARCHAR 类型，url的字符串。
- partToExtract VARCHAR 类型，解析后获取的值。
- key VARCHAR 类型，指的是参数名。

# 功能描述

解析url，获取partToExtract的值，如partToExtract=‘QUERY’，获取url参数key的值
partToExtract可取HOST、PATH、QUERY、REF、PROTOCOL、FILE、AUTHORITY、USERINFO。

## 注意：

参数为null返回null

# 示例

- 测试数据

| url1(VARCHAR)                                                                      | nullstr(VARCHAR) | 
|------------------------------------------------------------------------------------|------------------| 
| [http://facebook.com/path/p1.php?query=1](http://facebook.com/path/p1.php?query=1) | null             |

- 测试案例

```sql
SELECT PARSE_URL(url1, 'QUERY', 'query')    as var1,
       PARSE_URL(url1, 'QUERY')             as var2,
       PARSE_URL(url1, 'HOST')              as var3,
       PARSE_URL(url1, 'PATH')              as var4,
       PARSE_URL(url1, 'REF')               as var5,
       PARSE_URL(url1, 'PROTOCOL')          as var6,
       PARSE_URL(url1, 'FILE')              as var7,
       PARSE_URL(url1, 'AUTHORITY')         as var8,
       PARSE_URL(nullstr, 'QUERY')          as var9,
       PARSE_URL(url1, 'USERINFO')          as var10,
       PARSE_URL(nullstr, 'QUERY', 'query') as var11
FROM T1
```

- 测试结果

| var1(VARCHAR) | var2(VARCHAR) | var3(VARCHAR) | var4(VARCHAR) | var5(VARCHAR) | var6(VARCHAR) | var7(VARCHAR)        | var8(VARCHAR) | var9(VARCHAR) | var10(VARCHAR) | var11(VARCHAR) |
|---------------|---------------|---------------|---------------|---------------|---------------|----------------------|---------------|---------------|----------------|----------------|
| 1             | query=1       | facebook.com  | /path/p1.php  | null          | http          | /path/p1.php?query=1 | facebook.com  | null          | null           | null           |

