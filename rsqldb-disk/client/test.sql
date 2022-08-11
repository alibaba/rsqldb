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

CREATE TABLE test_cdc_sync
(
    `ticket_id`    BIGINT,
    `position`     VARCHAR,
    `odeum_name`   VARCHAR,
    `perform_name` VARCHAR,
    primary key (ticket_id)
) WITH (
      type = 'print'
      );

CREATE VIEW ticket_perform AS
SELECT t.id         AS ticket_id,
       t.`position`   AS `position`,
       p.name       AS perform_name,
       p.odeum_id   AS odeum_id
FROM ticket AS t
         JOIN perform FOR SYSTEM_TIME AS OF PROCTIME() AS p ON t.perform_id = p.id;

INSERT INTO test_cdc_sync
SELECT t.ticket_id    AS ticket_id,
       t.`position`     AS `position`,
       o.name         AS odeum_name,
       t.perform_name AS perform_name
FROM ticket_perform AS t
         JOIN odeum FOR SYSTEM_TIME AS OF PROCTIME() AS o ON t.odeum_id = o.id;


