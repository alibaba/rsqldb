# 语法

```sql
 BOOLEAN
IS_DECIMAL(VARCHAR str)
```

# 入参

- str VARCHAR 类型，普通字符串。

# 功能描述

str字符串是否可以转换成是其他数值，可以则返回TRUE，否则返回FALSE。

# 示例

- 测试数据

| a(VARCHAR) | b(VARCHAR) | c(VARCHAR) | d(VARCHAR) | e(VARCHAR) | f(VARCHAR) | g(VARCHAR) |
|------------|------------|------------|------------|------------|------------|------------|
| 1          | 123        | 2          | 11.4445    | 3          | asd        | null       |

- 测试案例

```sql
SELECT IS_DECIMAL(a) as boo1,
       IS_DECIMAL(b) as boo2,
       IS_DECIMAL(c) as boo3,
       IS_DECIMAL(d) as boo4,
       IS_DECIMAL(e) as boo5,
       IS_DECIMAL(f) as boo6,
       IS_DECIMAL(g) as boo7
FROM T1
```

- 测试结果

| boo1(BOOLEAN) | boo2(BOOLEAN) | boo3(BOOLEAN) | boo4(BOOLEAN) | boo5(BOOLEAN) | boo6(BOOLEAN) | boo7(BOOLEAN) |
|---------------|---------------|---------------|---------------|---------------|---------------|---------------|
| true          | true          | true          | true          | true          | false         | false         |

