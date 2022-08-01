-- 源表1
DROP TABLE ticket;
CREATE TABLE ticket (
                        id BIGINT,
                        perform_id BIGINT,
                        position VARCHAR(50),
                        gmt_modified TIMESTAMP,
                        primary key (id)
);
INSERT INTO ticket VALUES (5, 2, "r2c2", NOW());
UPDATE ticket SET position = "r2c4" WHERE id = 5;
DELETE FROM ticket WHERE id = 5;

-- 源表2
DROP TABLE odeum;
CREATE TABLE odeum (
                       id BIGINT,
                       name VARCHAR(50),
                       gmt_modified TIMESTAMP,
                       primary key (id)
);
INSERT INTO odeum VALUES (2, "B", NOW());

-- 源表3
DROP TABLE perform;
CREATE TABLE perform (
                         id BIGINT,
                         name VARCHAR(50),
                         odeum_id BIGINT,
                         gmt_modified TIMESTAMP,
                         primary key (id)
);
INSERT INTO perform VALUES (2, "b", 2, NOW());
UPDATE perform SET name = "c" WHERE id = 1;
DELETE FROM perform WHERE id = 2;

-- 目标表
DROP TABLE test_cdc_sync;
CREATE TABLE test_cdc_sync (
                               ticket_id BIGINT,
                               position VARCHAR(50),
                               odeum_name VARCHAR(50),
                               perform_name VARCHAR(50),
                               primary key (ticket_id)
);
SELECT * FROM test_cdc_sync;


CREATE VIEW ticket_perform AS SELECT
                                  t.id AS ticket_id,
                                  t.`position` AS `position`,
                                  p.name AS perform_name,
                                  p.odeum_id AS odeum_id
                              FROM ticket AS t
                                       JOIN perform FOR SYSTEM_TIME AS OF PROCTIME() AS p
                                            ON t.perform_id = p.id;

INSERT INTO test_cdc_sync SELECT
                              t.ticket_id AS ticket_id,
                              t.`position` AS `position`,
                              o.name AS odeum_name,
                              t.perform_name AS perform_name
FROM ticket_perform AS t
         JOIN odeum FOR SYSTEM_TIME AS OF PROCTIME() AS o
              ON t.odeum_id = o.id;

