# 语法格式：

```sql
INSERT INTO tableName
    [ (columnName[, columnName]*) ]
    queryStatement;
```

# 举例：

```sql
INSERT INTO tableName
SELECT *
FROM source_table
WHERE units > 1000;

INSERT INTO tableName(z, v)
SELECT c, d
FROM source_table;
```

# 说明：

- 单个流计算作业支持在一个SQL作业里面包含多个DML操作，同样也允许包含多个数据源、多个数据目标端、多个维表。例如在一个作业文件里面包含两段完全业务上独立的SQL，分别写出到不同的数据目标端。
- 流计算不支持单独的select 查询，必须有CREATE VIEW 或者是在 INSERT INTO内才能操作。
- INSERT INTO 支持UPDATE更新，例如向RDS的表插入一个KEY值，如果这个KEY值存在就会更新；如果不存在就会插入一条新的KEY值。

操作约束如下表：

| 表类型 | 操作约束                  |
|-----|-----------------------|
| 源表  | 只能引用(FROM)，不可执行INSERT |
| 维表  | 只能引用(JOIN)，不可执行INSERT |
| 结果表 | 仅支持INSERT操作           |
| 视图  | 只能引用(FROM)            |



