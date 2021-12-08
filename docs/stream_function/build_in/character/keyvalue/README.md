# 语法

```sql
VARCHAR KEYVALUE(VARCHAR str, VARCHAR split1, VARCHAR split2, VARCHAR key_name)
```

# 入参

- str VARCHAR 类型，字符串中的key-value对。
- split1 VARCHAR 类型，kv对的分隔符。**当split1为null，表示按照whitespace作为kv对的分割符;当split1的长度>1，split1仅表示分隔符的集合，每个字符都表示一个有效的分隔符**。
- split2 VARCHAR 类型，kv的分隔符。**当split2为null，表示按照whitespace作为kv的分割符;当split2的长度>1，split2仅表示分隔符的集合，每个字符都表示一个有效的分隔符**。
- key_name VARCHAR 类型kv的值。

# 功能描述

解析str字符串中的key-value对，匹配有split1(kv对的分隔符)和split2(kv的分隔符)key-value对。key_name返回对应的数值。key_name值不存在或者异常返回NULL。
**如果需要解析多个key对应的value值，可以使用**[**MULTI_KEYVALUE**](sql/builtin-functions/multikeyvalue.md)

# 样例

- 测试数据

| str(VARCHAR) | split1(VARCHAR) | split2(VARCHAR) | key1(VARCHAR) | 
| --- | --- | --- | --- | 
| k1=v1;k2=v2 | ; | = | k2 | 
| null | ; | = | k1 | 
| k1=v1;k2=v2 | ; | = | null |
| k1=v1 k2=v2 | null | = | k2 | 
| k1 v1;k2 v2 | ; | null | k2 | 
| k1:v1;k2:v2 | ; | : | k3 | 
| k1:v1abck2:v2 | abc | : | k2 | 
| k1:v1;k2=v2 | ; | := | k2 |

- 测试案例

```sql
SELECT KEYVALUE(str, split1, split2, key1) as `result`
FROM T1
```

- 测试结果

| result(VARCHAR) |
| --- |
| v2 |
| null |
| null |
| v2 |
| v2 |
| null |
| v2 |
| v2 |

