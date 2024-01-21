# 语法示例

流计算可以将消息队列作为流式数据输入，如下:

```sql
create table metaq_stream
(
    ip varchar header,
    x  varchar,
    y  varchar,
    z  varchar
) with (
      type = 'db',
      url = '',
      tableName = '',
      userName = '',
      password = ''
      batchSize = 1000,
      autoFlushSize = 300,
      autoFlushTimeGap = 1000
      );
```

| 参数名              | 是否必填 | 字段说明                                 | 默认值  |
|------------------|------|--------------------------------------|------|
| type             | 是    | 固定值，可以是db,rds                        |
| url              | 是    | jdbc连接地址                             |      |
| tableName        | 是    | 表名                                   |      |
| userName         | 是    | 用户名                                  | *    |
| password         | 是    | 密码                                   |      |
| batchSize        | 否    | 消息缓存输出，缓存的大小                         | 1000 |
| autoFlushSize    | 否    | 缓存启动线程异步刷新，当缓存条数>配置值时，会刷新缓存          | 300  |
| autoFlushTimeGap | 否    | 缓存启动线程异步刷新，当上次刷新时间到现在时间间隔>配置值时，会刷新缓存 | 1000 |

### 说明

- 实时计算写入Mysql数据库结果表原理：针对实时计算每行结果数据缓存，在刷新时，拼接成一行SQL语句-insert table(
  column1,column2,..)values(value1,value2,...),(value1,value2,...),(value1,value2,...)...，一次批量输入数据库，能最大的提高吞吐量。
- 如果想提高实时性，可以通过设置batchSize,autoFlushSize,autoFlushTimeGap三个参数来调整。如果

batchSize=1，会一条一输出，性能会大幅下降

- MySQL数据库支持自增主键。如果需要让实时计算写入数据支持自增主键，在DDL中不声明该自增字段即可。例如，ID是自增字段，实时计算DDL不写出该自增字段，则数据库在一行数据写入过程中会自动填补相关的自增字段。

# 环境变量

直接在sql中写参数值，既有泄漏的安全风险，对于专有云，idc输出场景，需要每个用户修改sql，这里提供了环境变量的概念：

- 可以在sql中，设置一个名字给参数，如password，可以设dipper.streams.db.password(这个名字随便取)
- 在配置文件中设置dipper.streams.db.password=真实ak的值
-

系统会自动化检查，检查逻辑：如果属性文件有dipper.streams.db.passwords，则用属性的值替换，检查环境变量是否有dipper.streams.db.password，如果有则用环境变量替换，如果没找到，则认为dipper.streams.db.password是真实值

```sql
create table metaq_stream
(
    ip varchar header,
    x  varchar,
    y  varchar,
    z  varchar
) with (
      type = 'db',
      url = 'dipper.streams.db.url',
      tableName = 'dipper.streams.db.tableName',
      userName = 'dipper.streams.db.userName',
      password = 'dipper.streams.db.password'
      batchSize = 1000,
      autoFlushSize = 300,
      autoFlushTimeGap = 1000
      );
```

mysql支持的环境变量： url,tableName,userName,password 属性文件配置：

```properties
dipper.streams.db.ur='',
dipper.streams.db.tableName='',
dipper.streams.db.userName='',
dipper.streams.db.password=''
```
