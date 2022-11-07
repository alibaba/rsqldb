# 快速启动单机模式

## 1. 编写任务代码

将任务代码保存为sql文件， 如test.sql 并放入jobs目录

```sql
CREATE FUNCTION json_concat as 'org.apache.rocketmq.streams.udf.JsonConcat';

CREATE TABLE `test_source`
(
    field_1 VARCHAR,
    field_2 VARCHAR,
    field_3 VARCHAR,
    field_4 VARCHAR,
    field_5 VARCHAR,
    field_6 VARCHAR,
    field_7 VARCHAR,
    field_8 VARCHAR,
    field_9 VARCHAR
) WITH (
      type = 'file',
      filePath = '/tmp/test.txt',
      isJsonData = 'true',
      msgIsJsonArray = 'false'
      );


-- 数据标准化

create view view_test as
select field_1
     , field_2
     , field_3
     , field_4
     , field_5
     , field_6
     , field_7
     , field_8
     , field_9
from (
         select field_1       as logtime
              , field_2       as uuid
              , field_3       as proc_name
              , field_4       as cmd
              , field_5       as pproc_name
              , field_6       as pcmd
              , field_7       as pexe
              , field_8       as ppexe
              , cast(field_9) as field9
         from test_source
     ) x
where (
                  lower(field_1) like '%.exe'
              or lower(field_2) like '%.exe'
              or field_3 like '_:/%'
              or field_4 like '_:/%'
              or field_5 like '//%'
              or field_6 like '//%'
          )
;

CREATE TABLE `test_sink`
(
    field_1 VARCHAR,
    field_2 VARCHAR,
    field_3 VARCHAR,
    field_4 VARCHAR,
    field_5 VARCHAR,
    field_6 VARCHAR,
    field_7 VARCHAR,
    field_8 VARCHAR,
    field_9 VARCHAR
) WITH (
      type = 'print'
      );

insert into test_sink
select field_1
     , field_2
     , field_3
     , field_4
     , field_5
     , field_6
     , field_7
     , field_8
     , field_9
from view_test

```

## 2. 任务启动

```shell
#启动过程中不加任务参数，则会将jobs中所有的任务都同时启动
bin/start-streams.sh

#启动过程加入第一个参数，即namespace参数， 则会将jobs目录下， namespace子目录下的任务都同时启动
bin/start-streams.sh namespace

# 启动过程中如果添加了namespace 和 任务名称， 则系统会在jobs目录中查询相关同名任务并启动，即便是
# 存在于不同的namespace的同名任务
bin/start-streams.sh namespace job_name

# 用户可以给每个任务去配置不同的jvm 参数
bin/start-streams.sh namespace job_name  '-Xms2048m -Xmx2048m -Xss512k'
```

运行日志会被打印在logs/stream.out文件中

## 3. 指定sql目录后启动任务

如果你的任务不在默认的jobs目录下， 则可以通过start-stream-sql.sh脚本来运行

```shell
#指定sql脚本的文件路径，启动实时任务， 此时任务的namespace和任务名称都与文件名相同
bin/start-stream-sql.sh sql_file_path

#除了指定sql脚本的文件路径，还指定了namespace， 此时任务使用指定的namespace来启动
bin/start-stream-sql.sh sql_file_path namespace

# 启动过程中如果添加了namespace和任务名称，则任务使用指定的namespace和job_name来启动
bin/start-stream-sql.sh sql_file_path namespace job_name

# 用户可以给每个任务去配置不同的jvm 参数
bin/start-stream-sql.sh sql_file_path namespace job_name  '-Xms2048m -Xmx2048m -Xss512k'
```

## 4. 停止任务

```shell
# 停止过程不加任何参数，则会将目前所有运行的任务同时停止
bin/stop-stream.sh

# 停止过程添加了任务名称， 则会将目前运行的所有同名的任务都全部停止
bin/stop-stream.sh job_name
```