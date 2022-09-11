CREATE TABLE `test_source`
(
    field_1 VARCHAR,
    field_2 VARCHAR,
    field_3 VARCHAR,
    field_4 VARCHAR
) WITH (
      type = 'file',
-- 需要根据自身填写data.txt的绝对路径
      filePath = '',
      isJsonData = 'true',
      msgIsJsonArray = 'false'
      );


CREATE TABLE `test_sink`
(
    field_1 VARCHAR,
    field_2 VARCHAR,
    field_3 VARCHAR,
    field_4 VARCHAR
) WITH (
      type = 'print'
      );

insert into test_sink
select field_1
     , field_2
     , field_3
     , field_4
from test_source where field_1='1';
