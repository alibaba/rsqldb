## 语法

```sql
VARCHAR REGEXP_REPLACE(VARCHAR str, VARCHAR pattern, VARCHAR replacement)
```

## 入参

- str VARCHAR 类型,指定的字符串。
- pattern VARCHAR 类型，被替换的字符串。
- replacement VARCHAR 类型,用于替换的字符串。

> 注：正则常量请按照 Java 代码来写，因为SQL常量字符串会原原本本地 codegen 成 Java 代码。所以如果要描述一个数字(\d)，需要写成 '\d'，也就是像在 Java 中写正则一样。Java 正则请参考：
[Java 正则表达式](http://wiki.jikexueyuan.com/project/java/regular-expressions.html)，
[Java Pattern](https://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html)

## 功能描述

用字符串replacement替换字符串str中正则模式为pattern的子串，返回新的字符串。正在匹配替换, 参数为null或者正则不合法返回null.

## 示例

- 测试数据 | str1(VARCHAR) | pattern1(VARCHAR) | replace1(VARCHAR) | | --- | --- | --- | | 2014-03-13 | - | "" | | null | - | "" | | 2014-03-13 | - | null | | 2014-03-13 | "" | s | | 2014-03-13 | ( | s | | 100-200 | (\d+) | num |


- 测试案例

```sql
SELECT REGEXP_REPLACE(str1, pattern1, replace1) as result
FROM T1
```

- 测试结果

  | result(VARCHAR) |
    | --- |
  | 20140313 |
  | null |
  | null |
  | 2014-03-13 |
  | null |
  | num-num |

