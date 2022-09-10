CREATE FUNCTION stringLengthUdf as 'com.test.rocketmqtest.function.customBlink.StringLengthUdf';

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
    `id`                        BIGINT,
    `num`                       BIGINT,
    `position_length`           INT
) WITH (
      type = 'print'
      );


INSERT INTO task_sink
SELECT
    `id`,
    `num`,
    stringLengthUdf(`position`)  AS  position_length
FROM source_function;
