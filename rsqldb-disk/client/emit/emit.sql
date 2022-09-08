CREATE TABLE user_clicks
(
    username        VARCHAR,
    click_url       VARCHAR,
    ts              TIMESTAMP,
    WATERMARK wk FOR ts as withOffset(ts, 2000) --为rowtime定义Watermark。
) WITH (
      type = 'rocketmq',
      topic = 'user_clicks',
      groupName = 'user_clicks',
      namesrvAddr = '127.0.0.1:9876',
      isJsonData = 'true',
      msgIsJsonArray = 'false'
      );

CREATE TABLE task_sink
(
    window_start        TIMESTAMP,
    username            VARCHAR,
    clicks              BIGINT
) WITH (
      type = 'print'
      );

CREATE VIEW test_view AS
SELECT
    TUMBLE_START(ts, INTERVAL '10' MINUTE)     as window_start,
    username                                as username,
    count(click_url)                        as clicks
FROM user_clicks
GROUP BY TUMBLE(ts, INTERVAL '10' MINUTE), username;

INSERT INTO task_sink
SELECT * FROM test_view
EMIT WITH DELAY '1' MINUTE BEFORE WATERMARK;



