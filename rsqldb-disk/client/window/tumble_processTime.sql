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

CREATE TABLE tumble_output
(
    window_start        TIMESTAMP,
    window_end          TIMESTAMP,
    username            VARCHAR,
    clicks              BIGINT
) with (
      type='print'
      );

INSERT INTO tumble_output
SELECT
    TUMBLE_START(ts, INTERVAL '1' MINUTE)       AS  window_start,
    TUMBLE_END(ts, INTERVAL '1' MINUTE)         AS  window_end,
    username                                    AS  username,
    COUNT(click_url)                            AS  clicks
FROM window_test
GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE), username;