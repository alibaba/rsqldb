MQTT 协议在边缘计算场景经常被使用， rocketmq-streams支持mqtt协议，也是想边缘计算场景迈进了一步

# 语法示例

流计算可以将消息队列作为流式数据输入，如下:

```sql
CREATE TABLE mqtt_content
(
    `content` VARCHAR
) WITH (
      type = 'mqtt',
      url = 'tcp://ip:port',
      clientId = '客户端ID',
      topic = 'Topic信息',
      username = '用户名',
      password = '密码',
      isJsonData = '数据格式是否json',
      cleanSession = '是否清理session',
      connectionTimeout = '连接超时时间',
      aliveInterval = '心跳间隔时间',
      automaticReconnect = '连接断开时，是否自动重连');
```

| 参数名                | 是否必填 | 字段说明                                                                                  | 默认值   |
|--------------------|------|---------------------------------------------------------------------------------------|-------|
| type               | 是    | 固定值，必须是mqtt                                                                           |       |
| url                | 是    | mqtt broker的地址， 格式为: 协议://IP:port                                                     |       |
| clientId           | 是    | 客户度ID                                                                                 |       |
| topic              | 是    | 需要订阅的topic信息                                                                          |       |
| username           | 否    | 当mqtt需要进行鉴权时，需要注明username和password参数                                                  |       |
| password           | 否    | 当mqtt需要进行鉴权时，需要注明username和password参数                                                  |       |
| isJsonData         | 否    | 消息是否是json格式                                                                           | true  |
| cleanSession       | 否    | 当客户端连接重新建立时，原来的session信息是否保留，为true则保留，客户端可以从连接断开的时间点继续消费信息，否则则从当前时间点消费，连接断开这段时间的数据将丢失 | true  |
| connectionTimeout  | 否    | 连接超时时间                                                                                | 10(s) |
| aliveInterval      | 否    | 心跳的额间隔时间                                                                              | 60(s) |
| automaticReconnect | 否    | 当连接异常时，是否自动重连                                                                         | true  |

# 自定义解析

当消息非json，jsonarray，或分割符分割，需要业务方自己解析时，可以采用如下方式

```sql
CREATE FUNCTION json_array AS 'org.apache.rocketmq.streams.script.function.impl.flatmap.SplitJsonArrayFunction';

CREATE TABLE mqtt_content
(
    `content` VARCHAR
) WITH (
      type = 'mqtt',
      url = 'tcp://ip:port',
      clientId = 'test_client_1',
      topic = 'usr/Module/DataDistribution/+/+/broadcast/+/+/metric/+/+',
      username = 'username',
      password = 'password',
      isJsonData = 'false',
      cleanSession = 'false',
      connectionTimeout = '5',
      aliveInterval = '30',
      automaticReconnect = 'true');

CREATE TABLE mqtt_result
(
    window_start    TIMESTAMP,
    window_end      TIMESTAMP,
    `AttributeCode` VARCHAR,
    `avg_value`     double
) WITH (
      type = 'print');

CREATE VIEW temp_view_1 AS
SELECT JSON_VALUE(`content`, '$.Data') AS data
FROM mqtt_content;

CREATE VIEW temp_view_2 AS
SELECT AttributeCode,
       AttributeId,
       Quality,
       `Value`,
       AttibuteName,
       AssetId,
       AssetCode,
       `Timestamp`
FROM temp_view_1,
     LATERAL TABLE(json_array(data, 'AttributeCode', 'AttributeId', 'Quality', 'Value', 'AttibuteName', 'AssetId', 'AssetCode', 'Timestamp')) AS T(AttributeCode, AttributeId, Quality, `Value`, AttibuteName,AssetId, AssetCode, `Timestamp`);

INSERT INTO mqtt_result
SELECT TUMBLE_START(`Timestamp`, INTERVAL '1' MINUTE),
       TUMBLE_END(`Timestamp`, INTERVAL '1' MINUTE),
       AttributeCode,
       AVG(`Value`) AS avg_value
FROM temp_view_2
group by TUMBLE(ts, INTERVAL '1' MINUTE), AttributeCode;


```

- 把isJsonData设置成false，系统会把这个字节数组根据encoding转化成字符串，并赋值给content字段
- 系统提供多种解析函数（类似flink的udtf），包括grok，正则解析等，也可以自定义解析函数，参考自定函数部分，在create view完成字段的解析
