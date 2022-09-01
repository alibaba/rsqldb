CREATE TABLE user
(
    `id`            BIGINT,
    `name`          BIGINT,
    `click_time`    TIMESTAMP,
    primary key (id)
) WITH (
      type = 'rocketmq',
      topic = 'user',
      groupName = 'user',
      namesrvAddr = '127.0.0.1:9876',
      isJsonData = 'true',
      msgIsJsonArray = 'false'
      );


CREATE TABLE task_sink
(
    `name`          VARCHAR,
    `nums`          BIGINT
) WITH (
      type = 'print'
      );


CREATE VIEW test_view as
SELECT `name`, count(name) AS nums
FROM user
GROUP BY TUMBLE(`click_time`, INTERVAL '1' MINUTE), `name`;


INSERT INTO task_sink
SELECT *
FROM test_view;
