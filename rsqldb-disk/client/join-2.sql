CREATE TABLE ticket
(
    `id`           BIGINT,
    `perform_id`   BIGINT,
    `position`     VARCHAR,
    `gmt_modified` TIMESTAMP,
    primary key (id)
) WITH (
      type = 'rocketmq',
      topic = 'rsqldb-ticket',
      groupName = 'rsqldb-ticket',
      namesrvAddr = '127.0.0.1:9876',
      isJsonData = 'true',
      msgIsJsonArray = 'false'
      );

CREATE TABLE perform
(
    `id`           BIGINT,
    `name`         VARCHAR,
    `odeum_id`     BIGINT,
    `gmt_modified` TIMESTAMP,
    primary key (id)
) WITH (
      type = 'rocketmq',
      topic = 'rsqldb-perform',
      groupName = 'rsqldb-perform',
      namesrvAddr = '127.0.0.1:9876',
      isJsonData = 'true',
      msgIsJsonArray = 'false'
      );

CREATE TABLE task_sink
(
    ticket_id       BIGINT,
    `position`      VARCHAR,
    perform_name    VARCHAR,
    odeum_id        BIGINT
) WITH (
      type = 'print'
      );

CREATE VIEW test_view AS
SELECT t.id         AS ticket_id,
       t.`position` AS `position`,
       p.name       AS perform_name,
       p.odeum_id   AS odeum_id
FROM ticket AS t
         JOIN perform AS p ON t.perform_id = p.id;


INSERT INTO task_sink
SELECT ticket_id,
       `position`,
       perform_name,
       odeum_id
FROM test_view;







