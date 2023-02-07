# SQL使用示例

## 本文目的

粗略列举介绍RSQLDB目前支持的SQL类型，并给出示例，输出数据，以及对数据计算后得到的结果。帮助大家快速了解RSQLDB的功能和使用场景，为进一步了解RSQLDB打下基础。

## 启动步骤

- **本地安装RocketMQ 5.0版本，并启动**

    安装文档见[quick start](https://rocketmq.apache.org/zh/docs/quickStart/02quickstart);

- **启动RSQLDB**

  - clone代码
  ```shell
  git clone git@github.com:alibaba/rsqldb.git
  ```
  - 编译antlr4生成源码
  ```shell
  mvn clean compile -DskipTests
  ```
  - 启动
  
  启动程序入口方法
  ```xml
  com.alibaba.rsqldb.rest.Application
  ```
- **创建数据源topic**
  
  根据SQL任务创建数据源topic:
  ```shell
  sh bin/mqadmin updateTopic -c ${clusterName} -t ${topicName} -r 8 -w 8 -n 127.0.0.1:9876
  ```
- **提交SQL任务**

  可参考SQL示例
- **写入数据**
  
  启动RocketMQ生产者，向源topic写入数据；
- **观察结果**
  
  - 任务中select语句为计算语句，结果在日志中；
  - 任务中使用insert into 将结果写入到topic中，结果数据在目标topic中；

## SQL任务及输入输出
SQL语句可单独提交，可多条SQL一起提交；
### 过滤
#### SQL语句
```sql
CREATE TABLE `rocketmq_source`
(
    field_1 INT,
    field_2 VARCHAR,
    field_3 VARCHAR,
    field_4 VARCHAR
) WITH (
      topic = 'rsqldb-source',
      data_format='json'
      );
 

CREATE TABLE `task_sink_2`
(
    field_1 INT,
    field_2 VARCHAR,
    field_3 VARCHAR,
    field_4 VARCHAR
) WITH (
      topic = 'rsqldb-sink',
      data_format='json'
      );

insert into task_sink_2
select *
from rocketmq_source where field_1=1;
```

#### 输入
```xml
{"field_1":1,"field_2":"2","field_3":"3","field_4":"4","field_5":"5"}
{"field_1":"1","field_2":"2","field_3":"3","field_4":"4"}
{"field_1":"3","field_2":"2","field_3":"3","field_4":"4"}
{"field_1":"4","field_2":"2","field_3":"3","field_4":"4"}
{"field_1":"5","field_2":"2","field_3":"3","field_4":"4"}
```
#### 输出
```xml
{"field_1":1,"field_2":"2","field_3":"3","field_4":"4"}
```
#### 支持同类型SQL
```sql
-- field_1=1;
-- field_1=1 and field_2='2';
-- field_1=1 or field_2='2';
-- field_1=1 or field_2=`3`;
-- field_1=1 or field_2=\"3\";
-- field_1 is null;
-- field_1 between 1 and 10;
-- field_2 like '%testData'
-- in(`1q2`, "1", 1, "123");
-- in(`1q2`, "1", 1, "123") and field_2=2;
-- field_1<10 or field_2=2 and field_3=`1`; -> (field_1<10) or (field_2=2 and field_3=`1`);
```

### 聚合查询

#### SQL
```sql
CREATE TABLE `rocketmq_source`
(
  field_1 INT,
  field_2 VARCHAR,
  field_3 VARCHAR,
  field_4 VARCHAR
) WITH (
    topic = 'rsqldb-source',
    data_format='json'
    );


CREATE TABLE `task_sink_2`
(
  field_1 INT,
  field_2 VARCHAR,
  field_3 VARCHAR,
  field_4 VARCHAR
) WITH (
    topic = 'rsqldb-sink',
    data_format='json'
    );

insert into task_sink_2
select field_1, sum(field_2) 
from rocketmq_source where field_1=1;
```

#### 输入
```xml
{"field_1":1,"field_2":2,"field_3":"3","field_4":"4","field_5":"5"}
{"field_1":1,"field_2":"2","field_3":"3","field_4":"4"}
{"field_1":1,"field_2":4,"field_3":"3","field_4":"4"}
{"field_1":"1","field_2":2,"field_3":"1","field_4":"3"}
{"field_1":2,"field_2":3,"field_3":"1","field_4":"4"}
```

#### 输出
```xml
{"field_1":1,"field_2":2} 
{"field_1":1,"field_2":2} 
{"field_1":1,"field_2":6} 
```
- 说明：
  第二行输出相同结果是因为数据{"field_1":1,"field_2":"2","field_3":"3","field_4":"4"}满足条件field_1=1，但是field_2不满足，所以输出前一个值；

#### 其他同类型SQL
  
  - 支持聚合函数：SUM、MAX、MIN、AVG、COUNT
  - 支持Having；


### Group By

#### SQL
```sql
CREATE TABLE `sourceTable`
(
    position        VARCHAR,
    num             INT
) WITH (
      topic = 'groupBy-source',
      data_format='json'
      );


SELECT `position`, avg(num) AS nums FROM sourceTable GROUP BY position;
-- 结果写入到日志中；
```

#### 输入
```xml
{"position":"shenzhen","num":10}
{"position":"shenzhen","num":3}
{"position":"shanghai","num":9}
{"position":"beijing","num":8}
{"position":"shanghai","num":4}
{"position":"beijing","num":11}
{"position":"shenzhen","num":6}
{"position":"shanghai","num":5}
{"position":"beijing","num":8}
{"position":"shanghai","num":6}
```

#### 输出

```xml
{"position":"shenzhen","nums":10.0} 
{"position":"beijing","nums":8.0} 
{"position":"shanghai","nums":4.0} 
{"position":"shenzhen","nums":6.5} 
{"position":"shenzhen","nums":6.33} 
{"position":"beijing","nums":9.5} 
{"position":"beijing","nums":9.0} 
{"position":"shanghai","nums":4.5} 
{"position":"shanghai","nums":6.0} 
{"position":"shanghai","nums":6.0} 
```

### process_time类型的window

#### SQL
```sql
CREATE TABLE `sourceTable`
(
    position        VARCHAR,
    num             INT,
    ts      as      PROCTIME()
) WITH (
      topic = 'window-source',
      data_format='json'
      );

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

#### 输入
```xml
{"position":"shenzhen","num":6}
{"position":"shanghai","num":7}
{"position":"shanghai","num":4}
{"position":"shenzhen","num":7}
{"position":"shanghai","num":6}
{"position":"shenzhen","num":6}
```
#### 输出
```xml
[2023-02-03 16:24:25 - 2023-02-03 16:24:30](key="shenzhen", value={"position":"shenzhen","sumNum":19,"window_start":1675412665000,"window_end":1675412670000})
[2023-02-03 16:24:25 - 2023-02-03 16:24:30](key="shanghai", value={"position":"shanghai","sumNum":13,"window_start":1675412665000,"window_end":1675412670000})
```


### event_time的window

#### SQL
```sql
CREATE TABLE `sourceTable`
(
    position        VARCHAR,
    num             INT,
    ts              timestamp
  -- 使用数据本身时间
) WITH (
      topic = 'window-source',
      data_format='json'
      );

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

#### 输入
```xml
{"position":"shenzhen","num":6,"ts": 1674874800000}
{"position":"shanghai","num":7,"ts": 1674874800000}
{"position":"shanghai","num":6,"ts": 1674874803000}
{"position":"shenzhen","num":7,"ts": 1674874804000}
{"position":"shanghai","num":6,"ts": 1674874806000}
{"position":"shenzhen","num":6,"ts": 1674874807000}
```

#### 输出
```xml
[2023-01-28 11:00:05 - 2023-01-28 11:00:10](key="shenzhen", value={"position":"shenzhen","sumNum":6,"window_start":1674874805000,"window_end":1674874810000})
[2023-01-28 11:00:00 - 2023-01-28 11:00:05](key="shenzhen", value={"position":"shenzhen","sumNum":13,"window_start":1674874800000,"window_end":1674874805000})
[2023-01-28 11:00:05 - 2023-01-28 11:00:10](key="shanghai", value={"position":"shanghai","sumNum":6,"window_start":1674874805000,"window_end":1674874810000})
[2023-01-28 11:00:00 - 2023-01-28 11:00:05](key="shanghai", value={"position":"shanghai","sumNum":13,"window_start":1674874800000,"window_end":1674874805000})
```
- 注：

  1、可能因为rsqldb先收到时间大的数据，后收到时间小的数据，导致时间小的数据因为低于水位线而被丢弃。如果需要根据严格的时间顺序计算结果，需要使用顺序topic，或者将允许乱序时间调大；
  
  2、为了排除上次计算保留状态的影响，需要清理存储在RocketMQ中的计算状态；


### JOIN双流聚合

#### SQL
```sql
CREATE TABLE `leftJoin`
(
    name            VARCHAR,
    age             INT
) WITH (
      topic = 'join-1',
      data_format='json'
      );

CREATE TABLE `rightJoin`
(
    name            VARCHAR,
    gender          VARCHAR
) WITH (
      topic = 'join-2',
      data_format='json'
      );

SELECT 
    SUM(a.age)       as   totalAge,
    b.gender         as   gender
FROM leftJoin AS a JOIN rightJoin AS b ON a.name=b.name
GROUP BY b.gender
HAVING SUM(a.age) > 20
```

#### 输入
```xml
<!-- 写入join-1中 -->
{"name":"shenzhen","age":28}
{"name":"shanghai","age":19}

<!--写入join-2中-->
{"name":"shenzhen","gender":"male"}
{"name":"shanghai","gender":"male"}
```

#### 输出

```xml
(key="male", value={"totalAge":28,"gender":"male"})
```


