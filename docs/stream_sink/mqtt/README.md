# 语法示例

流计算可以将消息队列作为流式数据输入，如下:

```sql
CREATE TABLE mqtt_result
(
    x varchar,
    y varchar,
    z varchar
) WITH (
      type = 'mqtt',
      url = 'tcp://ip:port',
      clientId = '客户端ID',
      topic = 'Topic信息',
      username = '用户名',
      password = '密码');
```

| 参数名      | 是否必填 | 字段说明                                 | 默认值 |
|----------|------|--------------------------------------|-----|
| type     | 是    | 固定值，必须是mqtt                          |     |
| url      | 是    | mqtt broker的地址， 格式为: 协议://IP:port    |     |
| clientId | 是    | 客户度ID                                |     |
| topic    | 是    | 需要订阅的topic信息                         |     |
| username | 否    | 当mqtt需要进行鉴权时，需要注明username和password参数 |     |
| password | 否    | 当mqtt需要进行鉴权时，需要注明username和password参数 |     |

