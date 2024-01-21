Temporal Table是一张不断变化的表（不更新的表是变化表的一种特例）。如何查询/JOIN一张不断变化的表呢？
如果用传统的JOIN语法来表达`JOIN temporal_table ON xxx`，会导致多次运行得到的结果不一致。所以我们在查询/JOINTemporal
Table的时候，需要明确指明我们想查看的是Temporal Table的哪个时刻的快照。因此我们引入了 SQL:2011 的 Temporal Table 的语义。

# Temporal Table DDL

目前支持[mysql](../../stream_dim/mysql/README.md)，[文件](../../stream_dim/file/README.md)，[odps](../../stream_dim/odps/README.md)

```sql
CREATE TABLE white_list
(
    id   varchar,
    name varchar,
    age  int,
    PRIMARY KEY (id), -- 用做Temporal Table的话，必须有声明的主键
    PERIOD FOR SYSTEM_TIME
) with (
      type = 'xxx',
      ...
      )
```

## 说明

- 维表不需要声明索引，会自动根据join条件创建，这块和bink略有不同
- 支持在条件的维表字段使用函数，但此字段不会索引
- 支持非等值比较，建议至少有一个等值比较
- 如果没有不带函数的等值比较，会进行全表的for循环匹配，性能会非常差，请谨慎使用
- 支持inner join和left join

## 示例

```sql
CREATE TABLE file_input1
(
    id   BIGINT,
    name VARCHAR,
    age  BIGINT
) WITH (
      type = 'flie',
      filePath = '/tmp/streams.txt'
      );

create table phoneNumber
(
    name        VARCHAR,
    phoneNumber bigint,
    primary key (name),
    PERIOD FOR SYSTEM_TIME
) with (
      type = 'rds'
      );

CREATE table result_infor
(
    id          bigint,
    phoneNumber bigint,
    name        VARCHAR
) with (
      type = 'rds'
      );

INSERT INTO result_infor
SELECT t.id,
       w.phoneNumber,
       t.name
FROM datahub_input1 as t
         JOIN phoneNumber FOR SYSTEM_TIME AS OF PROCTIME() as w
              ON t.name = w.name;
```


