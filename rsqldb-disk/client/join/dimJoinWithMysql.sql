CREATE TABLE ticket
(
    `id`                INT,
    `purchaser_id`      INT,
    `movie_name`        VARCHAR,
    `position`          VARCHAR,
    `time`              TIMESTAMP,
    primary key (id)
) WITH (
      type = 'rocketmq',
      topic = 'rsqldb-ticket',
      groupName = 'rsqldb-ticket',
      namesrvAddr = '127.0.0.1:9876',
      isJsonData = 'true',
      msgIsJsonArray = 'false'
      );

CREATE TABLE purchaser_dim
(
    `purchaser_id`      INT,
    `name`              VARCHAR,
    `gender`            VARCHAR,
    `age`               INT,
    primary key (purchaser_id)
)WITH (
    type = 'db',
    url='jdbc:mysql://localhost:3306/rocketmq_streams',
    userName='root',
    password='nize1989+',
    tableName='purchaser_dim',
    cacheTTLMs='60000'
     );

CREATE VIEW result_view AS
SELECT
    t.purchaser_id          AS purchaser_id,
    pd.name                 AS name,
    pd.gender               AS gender,
    t.movie_name            AS movie_name
FROM ticket as t JOIN purchaser_dim FOR SYSTEM_TIME AS OF PROCTIME() AS pd
ON t.purchaser_id = pd.purchaser_id;


CREATE TABLE result_table
(
    `purchaser_id`              INT,
    `name`                      VARCHAR,
    `gender`                    VARCHAR,
    `movie_name`                VARCHAR
) WITH (
        type = 'print'
      );

INSERT INTO result_table
SELECT *
FROM result_view;

