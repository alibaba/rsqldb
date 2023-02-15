## RSQLDB

The database build for stream processing.


## Overview

RSQLDB is a database for stream processing build on the of RocketMQ. It is distributed, highly available，scalable. SQL can be used to define a stream processing task, and
RSQLDB parse it into a stream-processing task. RSQLDB offers the fellow core features:

- Restful API - create, query, stop, stream-processing tasks;
- Standard SQL - describe the stream processing task with standard sql;
- Materialized views - incremental calculation on the of stream;


## Deploy

### Run RocketMQ 5.0 locally

Steps are as follows:
- **Install Java**

- **Download RocketMQ**

- **Start NameServer**

- **Start Broker**

More details can be obtained at [quick start](https://rocketmq.apache.org/zh/docs/quickStart/02quickstart);


### Run RSQLDB

#### From source code:

- Git clone and compile
```shell
git clone git@github.com:alibaba/rsqldb.git

#compile antlr4 file to source code:
mvn clean compile -DskipTests
```
* Run the entrance method:
```java
  com.alibaba.rsqldb.rest.Application
````

#### From distribution package
- Download distribution package
- Unzip package
```shell
unzip rsqldb-distribution.zip
```
- Start rsqldb
```shell
cd rsqldb && sh bin/start.sh
```

## Use Cases and Examples

### Filter

```shell
select *
from sourceTable where age>20 and name like '%mack';
```


### Join

```shell
SELECT Websites.name as `count`, Websites.url as url, SUM(access_log.count) AS nums 
FROM access_log 
WHERE access_log.`count` > 100
INNER JOIN Websites ON access_log.site_id=Websites.id and access_log.url=Websites.url
```

### Window

```sql
select 
TUMBLE_START(ts, INTERVAL '5' SECOND)       AS window_start,
TUMBLE_END(ts, INTERVAL '5' SECOND)         AS window_end,
position                                    AS position,
sum(num)                                    AS sumNum
from  sourceTable
where num > 5
group by TUMBLE(ts, INTERVAL '5' SECOND), position
having sum(num) < 20;
```

- More examples can be found [here](docs/sql_example.md).
