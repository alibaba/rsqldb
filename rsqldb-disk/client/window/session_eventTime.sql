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
    window_end          TIMESTAMP,
    username            VARCHAR,
    clicks              BIGINT
) WITH (
      type = 'print'
      );

INSERT INTO task_sink
SELECT
    SESSION_START(ts, INTERVAL '30' SECOND)     as window_start,
    SESSION_END(ts, INTERVAL '30' SECOND)       as window_end,
    username                                    as username,
    COUNT(click_url)                            as clicks
FROM user_clicks
GROUP BY SESSION(ts, INTERVAL '30' SECOND), username;
