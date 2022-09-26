为了 blink规则可以无缝迁移到RSQLDB，我们做了blink sql的语法兼容，在云盾里的sql都是可以直接迁移的，当然由于时间短，可能会有很多未考虑到的场景，大家使用中如果有问题可以随时联系。<br />下表为blink与rocketmq-stream的语法比较，其中rocketmq-stream标 * 的列语法与blink完全相同，未标 * 列对于语法支持层面进行了说明，标红部分表示rocketmq-stream还未支持。

| 语句          | 语法 | blink | rocketmq-stream | 说明 |
|-------------| --- | --- | --- | --- |
| CREATE      | source | * | rocketmq，kafka，sls，db，file |  |
| CREATE      | sink | * | rocketmq，kakfa，sls，file，print，db，es |  |
| CREATE      | 维表 | * | mysql |  |
| CREATE      | 函数 | * | * |  |
| CREATE      | view | * | * |  |
| DML         | INSERT INTO | * | * |  |
| DML         | Emit | * | * |  |
| Query       | SELECT | * | * |  |
| Query       | Where | * | * | distinct默认实现是尽量去重，基于单实例去重，全局有重复数据，性能比全局去重有指数提高。<br />如果想全局去重，通过修改配置切换streams.distinct.globle=true |
| Query            | Group Aggregate | * | * |  |
| Query           | GROUPING SETS | * | 未实现 |  |
| Query           | UNION ALL | * | * | 目前UNION ALL和UNION是一个语义，UNION未做去重 |
| Query           | HAVING | * | * |  |
| Query           | 双流JOIN | * | INNER JOIN, LEFT JOIN | 未实现RIGHT JOIN, FULL JOIN, ANTI JOIN, SEMI JOIN |
| Query           | Temporal Table JOIN | * | mysql，全部缓存策略 | 不支持热点缓存和直接远程访问侧露。<br />做了压缩设置，尤其情报，千万300m内存 |
| Query           | Time-windowed JOIN | * | 会转化成窗口双流join，窗口大小默认1小时 | 默认时间是1小时，可通过配置调整 |
| Query           | TopN | * | * |  |
| Query           | 去重 | * | * | 当order by 是proctime()时，会默认用尽量去重，基于单实例去重，全局有重复数据，性能比全局去重有指数提高。可通过配置改变实现 |
| Query           | CEP | * |  |  |
| Aggregations | Group Aggregate | * | * | 默认时间是1小时，通过配置调整 |
|   Aggregations          | Window Aggregate | *	 | * | 全局窗口通过把groupby key设置成一样的实现 |
|   Aggregations          | Over Aggregate | * | * | 仅支持topN的排序和去重 |
| 内置函数 | 逻辑操作函数 | *	 | * |  |
|  内置函数           | 字符串函数 | *	 | * |  |
|  内置函数           | 数学函数 | *	 | * |  |
|  内置函数           | 条件函数 | *	 | * |  |
|  内置函数           | 日期函数 | *	 | 4个未实现，主要是时区转化的 | l	TIMESTAMPADD<br />l	DATE_FORMAT_TZ<br />l	TO_TIMESTAMP_TZ<br />l	CONVERT_TZ |
|  内置函数           | 表值函数 | *	 | 3个未实现 | l	GENERATE_SERIES<br />l	JSON_TUPLE<br />l	MULTI_KEYVALUE |
|  内置函数           | 聚合函数 | * | 实现了count，min，max，sum，avg。其他未实现 | l	APPROX_PERCENTILE<br />l	APPROX_PERCENTILE<br />l	CONCAT_AGG<br />l	VAR_POP<br />l	STDDEV_POP<br />l	LAST_VALUE<br />l	FIRST_VALUE |
|  内置函数           | 其他函数 | * | * | 如果函数中用到flink的context内容，无法兼容。其他场景可兼容 |
| 自定义函数       | UDF | * | * |  |
|   自定义函数           | UDTF | * | * |  |
|   自定义函数           | UDAF | * | * |  |
|   自定义函数           | pythonudf | * | 有python的函数实现，暂未开放 |  |
