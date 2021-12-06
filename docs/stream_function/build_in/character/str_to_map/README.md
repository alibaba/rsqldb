## 语法

```sql
MAP
STR_TO_MAP(VARCHAR text)
MAP STR_TO_MAP(VARCHAR text, VARCHAR listDelimiter, VARCHAR keyValueDelimiter)
```

## 功能描述

使用listDelimiter将text分隔成K-V对，然后使用keyValueDelimiter分隔每个K-V对，组装成 MAP 返回。默认 listDelimiter 为 `,`, keyValueDelimiter为 `=`。

#### 注意 这里的 Delimiter 使用的是 java 的 regex(参考 [正则表达式的写法](http://site.alibaba.net/blink/blink-doc/archives/zheng-ze-biao-da-shi-de-xie-fa.html)), 遇到特殊字符需要转义

####    

## 参数说明：

- text VARCHAR 类型，输入文本。
- listDelimiter VARCHAR 类型，用来将 text 分隔成 K-V 对。默认 `,`。
- keyValueDelimiter VARCHAR 类型，用来分隔每个 key 和 value。默认 `=`。

## 示例

```sql
SELECT STR_TO_MAP('k1=v1,k2=v2')['k1'] as a
FROM T1
```

结果:

| a(VARCHAR) |
| --- |
| v1 |

