# 语法

```sql
VARCHAR JSON_VALUE(VARCHAR content, VARCHAR path)
```

# 入参

- content VARCHAR类型, 需要解析的JSON对象，使用字符串表示,字符串不包含"$","[]","*",".".
- path VARCHAR类型，解析JSON的路径表达式. 目前Path支持如下表达式:

| 符号 | 功能 | 
| --- | --- | 
| $ | 根对象 | 
| [] | 数组下标 | 
| * | 数组通配符 | 
| . | 取子元素 |

# 功能描述

从JSON字符串中提取指定path的值，不合法的json和null都统一返回null。 注：自定义的path需要使用单引号，如下所示：

```javascript
JSON_VALUE(json, '$.passenger_name')
AS
xxx
```

# 示例

- 测试数据

| id(INT) | json(VARCHAR) | path(VARCHAR) | 
| --- | --- | --- | 
| 1 | [10, 20, [30, 40]] | $[2][*] | 
| 2 | {"aaa":"bbb","ccc":{"ddd":"eee","fff":"ggg","hhh":["h0","h1","h2"]},"iii":"jjj"} | $.ccc.hhh[*] | 
| 3 | {"aaa":"bbb","ccc":{"ddd":"eee","fff":"ggg","hhh":["h0","h1","h2"]},"iii":"jjj"} | $.ccc.hhh[1] | 
| 4 | [10, 20, [30, 40]] | NULL | 
| 5 | NULL | $[2][*] | 
| 6 | "{xx]" | $[2][*] |

- 测试SQL

```sql
SELECT id， JSON_VALUE(json, path) AS `value`
FROM T1
```

- 测试结果

| id (INT) | value (VARCHAR) |
| --- | --- |
| 1 | [30,40] |
| 2 | ["h0","h1","h2"] |
| 3 | h1 |
| 4 | NULL |
| 5 | NULL |
| 6 | NULL |

