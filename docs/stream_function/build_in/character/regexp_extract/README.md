# 语法

```sql
VARCHAR REGEXP_EXTRACT(VARCHAR str, VARCHAR pattern, INT index)
```

# 入参

- str VARCHAR 类型，指定的字符串。
- pattern VARCHAR 类型，匹配的字符串。
- index INT 类型 ，第几个被匹配的字符串。

> 注：正则常量请按照 Java 代码来写，因为SQL常量字符串会原原本本地 codegen 成 Java 代码。所以如果要描述一个数字(\d)
> ，需要写成 '\d'，也就是像在 Java 中写正则一样。Java 正则请参考：
[Java 正则表达式](http://wiki.jikexueyuan.com/project/java/regular-expressions.html)，
[Java Pattern](https://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html)

# 功能描述

使用正则模式pattern匹配抽取字符串str中的第index个子串，index 从1开始 正在匹配提取, 参数为null或者正则不合法返回null。

# 示例

- 测试数据

| str1 (VARCHAR) | pattern1(VARCHAR) | index1 (INT) | 
|----------------|-------------------|--------------| 
| foothebar      | foo(.*?)(bar)     | 2            | 
| 100-200        | (\d+)-(\d+)       | 1            | 
| null           | foo(.*?)(bar)     | 2            | 
| foothebar      | null              | 2            | 
| foothebar      | ""                | 2            | 
| foothebar      | (                 | 2            |

- 测试案例

```sql
SELECT REGEXP_EXTRACT(str1, pattern1, index1) as result
FROM T1
```

- 测试结果

| result(VARCHAR) |
|-----------------|
| bar             |
| 100             |
| null            |
| null            |
| null            |
| null            |

