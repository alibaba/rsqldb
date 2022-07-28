CREATE TABLE `rocketmq_source`
(
    field_1 VARCHAR,
    field_2 VARCHAR,
    field_3 VARCHAR,
    field_4 VARCHAR
) WITH (
      type = 'rocketmq',
      topic = 'rsqldb-source',
      groupName = 'rsqldb-group',
      namesrvAddr = '127.0.0.1:9876',
      isJsonData = 'false',
      msgIsJsonArray = 'false'
      );


-- 数据标准化

create view rocketmq_view as
select field_1
     , field_2
     , field_3
     , field_4
from (
         select field_1
              , field_2
              , field_3
              , field_4
         from rocketmq_source
     )
where (field_1='1');


CREATE TABLE `task_sink_2`
(
    field_1 VARCHAR,
    field_2 VARCHAR,
    field_3 VARCHAR,
    field_4 VARCHAR
) WITH (
      type = 'print'
      );

insert into task_sink_2
select field_1
     , field_2
     , field_3
     , field_4
from rocketmq_view
