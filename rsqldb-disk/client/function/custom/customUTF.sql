CREATE FUNCTION getCurrentTime as 'com.test.rocketmqtest.function.custom.UDFTest';

CREATE TABLE source_function
(
    `id`           BIGINT,
    `num`          BIGINT,
    `position`     VARCHAR
) WITH (
      type = 'rocketmq',
      topic = 'source_function',
      groupName = 'source_function',
      namesrvAddr = '127.0.0.1:9876',
      isJsonData = 'true',
      msgIsJsonArray = 'false'
      );


CREATE TABLE task_sink
(
    `id`                BIGINT,
    `num`               BIGINT,
    `position`          VARCHAR,
    `function_time`     VARCHAR
) WITH (
      type = 'print'
      );


INSERT INTO task_sink
SELECT
    `id`,
    `num`,
    `position`,
    getCurrentTime()    AS function_time
FROM source_function;




