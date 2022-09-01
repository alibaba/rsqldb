-- 按照位置聚合每个位置的num
CREATE TABLE source_function_0
(
    `id`           BIGINT,
    `num`          BIGINT,
    `position`     VARCHAR,
    `gmt_modified` TIMESTAMP,
    primary key (id)
) WITH (
      type = 'rocketmq',
      topic = 'source_function_0',
      groupName = 'source_function_0',
      namesrvAddr = '127.0.0.1:9876',
      isJsonData = 'true',
      msgIsJsonArray = 'false'
      );


CREATE TABLE task_sink
(
    `nums`          BIGINT,
    `position`      VARCHAR
) WITH (
      type = 'print'
      );

-- 验证count/avg/sum/max/min
CREATE VIEW test_view AS
SELECT `position`, count(num) AS nums
FROM source_function_0
GROUP BY `position`;


INSERT INTO task_sink
SELECT *
FROM test_view;
