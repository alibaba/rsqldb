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
      filePath = '/Users/nize/code/github/rsqldb/rsqldb-client/src/main/resources/data.txt',
      isJsonData = 'false',
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
         select field_1
              , field_2
              , field_3
              , field_4
              , field_5
              , field_6
              , field_7
              , field_8
              , field_9
         from test_source
     )
where (
              field_1='1'
          );

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
