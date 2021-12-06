GROUP BY 语句用于结合统计函数，根据一个或多个列对结果集进行分组

## 语法格式：

```sql
SELECT [ ALL | DISTINCT ]
    { * | projectItem [, projectItem ]* }
FROM tableExpression
    [
GROUP BY { groupItem [, groupItem ]* } ];
```

## 例子：

```sql
SELECT Customer, SUM(OrderPrice)
FROM XXXX
GROUP BY Customer;
```
