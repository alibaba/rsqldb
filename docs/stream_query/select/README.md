# 语法格式

```sql
SELECT [ ALL | DISTINCT ]
    { * | projectItem [, projectItem ]* }
FROM tableExpression;
```

## 示例

```sql
SELECT *
FROM 表名;

SELECT a, c AS d
FROM 表名;

SELECT DISTINCT a
FROM 表名;
```

# 子查询

普通的SELECT是从几张表中读数据，如SELECT column_1, column_2 … FROM table_name，但查询的对象也可以是另外一个SELECT操作，需要注意的是子查询必须加别名。代码如下：

```sql
INSERT INTO result_table
SELECT *
from (
         SELECT t.a,
                sum(t.b) AS sum_b
         FROM t1 t
         GROUP BY t.a) t1
WHERE t1.sum_b > 100;
```
