# 语法示例

流计算可以将消息队列作为流式数据输入，如下:

```sql
create table metaq_stream
(
    _facility    varchar,
    _hostName    varchar,
    _hostAddress varchar,
    _level       varchar,
    _msg         varchar,
    _date        varchar,
    _tag         varchar,
    _pid         varchar
) with (
      type = 'syslog',
      port = '12345',
      protol = 'udp',
      client_ips = '192.168.1.1-192.168.1.10'
      );
```

| 参数名        | 是否必填 | 字段说明                                                                    | 默认值     |
|------------|------|-------------------------------------------------------------------------|---------|
| type       | 是    | 固定值，必须是syslog                                                           |
| port       | 是    | udp或tcp                                                                 | udp     |
| protol     | 是    | udp或tcp                                                                 | udp或tcp |
| client_ips | 是    | 消息发送源的ip或域名列表，支持ip范围-192.168.1.1-192.168.1.10，ip端-192.168.0.0/22，和正则表达式 | *       |

-

这个source会启动一个syslog的server，相同port和protol的source，无论创建多少次，一个实例也只会启动一个，不同数据源的syslog日志，可能有不同的业务逻辑，通过配置client_ips来区分不同的syslog日志

# 输入字段

- 因为syslog是标准的格式，create 字段名，也必须按例子中的字段名定义 | 属性字段 | 说明 | | --- | --- | | _facility |
  优先级 | | _hostName | host name | | _hostAddress | client ip | | _level | 级别 | | _msg | syslog的消息 | | _date |
  日志时间，格式2010-09-09 12:12:12 | | _tag | 标签 | | _pid | 产生日志的进程id |

# 自定义解析

当消息非json，jsonarray，或分割符分割，需要业务方自己解析时，可以采用如下方式

```sql
create
grok MyParser  as org.apache.rocketmq.streams.script.annotation.Function;

create table metaq_stream
(
    _facility    varchar,
    _hostName    varchar,
    _hostAddress varchar,
    _level       varchar,
    _msg         varchar,
    _date        varchar,
    _tag         varchar,
    _pid         varchar
) with (
      type = 'syslog',
      port = '12345',
      protol = 'udp',
      client_ips = '192.168.1.1-192.168.1.10'
      );

create view paser as
select t.a,
       t.b,
       t.c
from metaq_stream,
     LATERAL table(grok(_msg,'解析语句')) as t(a,b,c）;

```

- 这里的_msg是业务字段，可以通过上述方式进行解析

# 注意事项

如果sql中用到了统计，join Dipper是基于消息队列做shuffle的，在数据源不是消息队列的场景，需要配置shuffle的消息队列，建议用rocketmq，kafka和sls也是支持的，具体配置如下：

```properties

#window
异步消息分发需要的消息队列类型
dipper.window.shuffle.dispatch.channel.type=metaq
#如果是metaq，统计和join分别用的topoc
dipper.window.shuffle.dispatch.channel.topic=TOPIC_DIPPER_WINDOW_STATISTICS
#动态生成的消息队列的属性的key值，这个值会被动态赋值
dipper.window.shuffle.dispatch.channel.dynamic.property=tag,group
dipper.window.shuffle.dispatch.channel.thread.max.count=8
```

- dipper.window.shuffle.dispatch.channel.type，支持metaq，rocketmq，sls，kafka，和用户自定义实现AbstractSupportShuffleSource这个类的source。
- 后面配置数据源的参数，前缀是dipper.window.shuffle.dispatch.channel. 后面按选择的数据源参数需求配置参数。
- dipper.window.shuffle.dispatch.channel.dynamic.property=tag,group 这块直接配置即可，不需要修改

