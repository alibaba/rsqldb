CREATE TABLE window_test
(
    username            VARCHAR,
    click_url           VARCHAR,
    ts          as      PROCTIME()
) WITH (
      type = 'rocketmq',
      topic = 'window_test',
      groupName = 'window_test',
      namesrvAddr = '127.0.0.1:9876',
      isJsonData = 'true',
      msgIsJsonArray = 'false'
      );

CREATE TABLE session_output
(
    window_start        TIMESTAMP,
    window_end          TIMESTAMP,
    username            VARCHAR,
    clicks              BIGINT
) with (
      type='print'
      );

INSERT INTO session_output
SELECT
    SESSION_START(ts, INTERVAL '30' SECOND)     as window_start,
    SESSION_END(ts, INTERVAL '30' SECOND)       as window_end,
    username                                    as username,
    COUNT(click_url)                            as clicks
FROM window_test
GROUP BY SESSION(ts, INTERVAL '30' SECOND), username;
