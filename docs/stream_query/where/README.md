如需有条件地从表中选取数据，可将 WHERE 子句添加到 SELECT 语句。

# 语法

```sql
SELECT [ ALL | DISTINCT ]
    { * | projectItem [, projectItem ]* }
FROM tableExpression
    [
WHERE booleanExpression ];
```

下面的运算符可在 WHERE 子句中使用：

| 操作符 | 描述   |
|-----|------|
| =   | 等于   |
| <>  | 不等于  |
| >   | 大于   |
| > = | 大于等于 |
| <   | 小于   |
| <=  | 小于等于 |

# 示例

```sql
SELECT *
FROM XXXX
WHERE City = 'Beijing'
```
