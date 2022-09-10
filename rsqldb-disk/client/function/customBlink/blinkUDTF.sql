CREATE FUNCTION parseUdtf AS 'com.test.rocketmqtest.function.customBlink.ParseUdtf';

CREATE TABLE source_function
(
    `id`           BIGINT,
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
    `name`                      VARCHAR,
    `num`                       BIGINT,
    `age`                       INT
) WITH (
      type = 'print'
      );


INSERT INTO task_sink
SELECT
    SF.id       AS id,
    T.a         AS name,
    T.b         AS num,
    T.c         AS age
FROM source_function AS SF,
LATERAL TABLE(parseUdtf(`position`)) AS T(`a`, `b`, `c`);