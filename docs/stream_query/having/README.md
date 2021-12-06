## HAVING 子句

在 SQL 中增加 HAVING 子句原因是，WHERE 关键字无法与合计函数一起使用。

## 语法

```sql
   SELECT [ ALL | DISTINCT ]
       { * | projectItem [, projectItem ]* }
   FROM tableExpression
       [
   WHERE booleanExpression ]
       [
   GROUP BY { groupItem [, groupItem ]* } ]
       [
   HAVING booleanExpression ];
```

## 例子

```sql
SELECT Customer, SUM(OrderPrice)
FROM XXX
GROUP BY Customer
HAVING SUM(OrderPrice) < 2000;
```
