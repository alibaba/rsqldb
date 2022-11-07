# 语法

```sql
CONCAT_AGG
([linedelimiter,] value )
```

# 入参

连接符，目前只支持字符串常量。

# 功能描述

链接字符串连接本批对应字段,默认连接符"\n"。返回值VARCHAR类型连接完成后新生成的字符串。

# 示例

- 测试数据

| b(VARCHAR) | c(VARCHAR) |
|---|---| 
| Hi | milk | 
| Hi | milk | 
| Hi | milk | 
| Hi | milk | 
| Hi | milk | 
| Hi | milk | 
| Hello | cola | 
| Hello | cola | 
| Happy | suda | 
| Happy | suda |

- 测试案例

```sql
SELECT b,
       concat_agg(c)      as var1,
       concat_agg('-', c) as var2
FROM MyTable
GROUP BY b
```

测试结果

| b (VARCHAR) | var1(VARCHAR) | var2(VARCHAR) |
| --- | --- | --- |
| Hi | milk\nmilk\nmilk\nmilk\nmilk\nmilk\n | milk-milk-milk-milk-milk-milk |
| Hello | cola\ncola\n | cola-cola |
| Happy | suda\nsuda\n | suda-suda |

