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


CREATE TABLE odeum
(
    `id`           BIGINT,
    `name`         VARCHAR,
    `gmt_modified` TIMESTAMP,
    primary key (id)
) WITH (
      type = 'rocketmq',
      topic = 'rsqldb-odeum',
      groupName = 'rsqldb-odeum',
      namesrvAddr = '127.0.0.1:9876',
      isJsonData = 'true',
      msgIsJsonArray = 'false'
      );


CREATE TABLE task_sink
(
    ticket_id    BIGINT,
    `position`   VARCHAR,
    odeum_name   VARCHAR,
    perform_name VARCHAR
) WITH (
      type = 'print'
      );

CREATE VIEW test_view AS
SELECT t.id         AS ticket_id,
       t.`position` AS `position`,
       p.name       AS perform_name,
       p.odeum_id   AS odeum_id
FROM ticket AS t JOIN perform FOR SYSTEM_TIME AS OF PROCTIME() AS p
ON t.perform_id = p.id;


CREATE VIEW result_view AS
SELECT
    a.ticket_id     AS ticket_id,
    a.`position`    AS `position`,
    b.name          AS odeum_name,
    a.perform_name  AS perform_name
FROM test_view as a JOIN odeum FOR SYSTEM_TIME AS OF PROCTIME() AS b
ON a.odeum_id = b.id;


INSERT INTO task_sink
SELECT ticket_id,
       `position`,
       odeum_name,
       perform_name
FROM result_view;
