CREATE TABLE source_condition_0
(
    `id`           BIGINT,
    `num`          BIGINT,
    `position`     VARCHAR,
    `gmt_modified` TIMESTAMP,
    primary key (id)
) WITH (
      type = 'rocketmq',
      topic = 'source_condition_0',
      groupName = 'source_condition_0',
      namesrvAddr = '127.0.0.1:9876',
      isJsonData = 'true',
      msgIsJsonArray = 'false'
      );


CREATE TABLE task_sink
(
    `id`           BIGINT,
    `num`          BIGINT,
    `position`     VARCHAR,
    `gmt_modified` TIMESTAMP
) WITH (
      type = 'print'
      );

CREATE VIEW test_view as
SELECT *
FROM source_condition_0
WHERE id >=5;


INSERT INTO task_sink
SELECT *
FROM test_view;
