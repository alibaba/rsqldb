# Emit 策略

Emit 策略是指在流计算 SQL 中，query的输出策略（如能忍受的延迟）可能在不同的场景有不同的需求，而这部分需求，传统的 ANSI SQL
并没有对应的语法支持。比如用户需求："
1小时的时间窗口，窗口触发之前希望每分钟都能看到最新的结果，窗口触发之后希望不丢失迟到一天内的数据"
（如果1小时窗口内的统计结果没变，就不更新输出结果, 有变化才会输出更新结果）。针对这类需求，抽象出了 Emit 语法，并扩展到了 SQL
语法。 ​

# Emit 用途

Emit 语法的用途目前总结起来主要提供了：控制延迟、数据精确性，两方面的功能。

1. 控制延迟。针对大窗口，设置窗口触发之前的 Emit 输出频率，减少用户看到结果的延迟。
1. 数据精确性。不丢弃窗口触发之后的迟到的数据，修正输出结果。

**在选择 Emit 策略时，还需要与处理开销进行权衡。因为越低的输出延迟、越高的数据精确性，都会带来越高的计算开销。**

# Emit 语法

Emit 语法是用来定义输出的策略，即是定义在输出（INSERT INTO）上的动作。 当未配置时，保持原有默认行为，即 window 触发时向下游发送结果。
​

```sql
INSERT INTO tableName
    query EMIT strategy [, strategy]*

strategy ::= {
WITH DELAY timeInterval}
    [BEFORE WATERMARK | AFTER WATERMARK]
    timeInterval : := 'string' timeUnit
```

- `WITH DELAY` : 声明能忍受的结果延迟，即按指定 interval 进行间隔输出。
- `WITHOUT DELAY` : Dipper不支持这个策略，必须有interval才可以，interval最小5秒
- `BEFORE WATERMARK` : 窗口结束之前的策略配置，即 watermark 触发之前。
- `AFTER WATERMARK` : 窗口结束之后的策略配置，即 watermark 触发之后。

注：

1. 其中 strategy 可以定义多个，同时定义 before 和 after 的策略。 但不能同时定义两个 before 或 两个 after 的策略。
1. 这个策略会影响所有的统计，join，窗口操作，会对这个sql的所有统计join生效

# 例子：

```sql
-- 窗口结束之前，按1分钟延迟输出，窗口结束之后无延迟输出
EMIT
WITH DELAY '1' MINUTE BEFORE WATERMARK,
 	WITH DELAY '1' MINUTE AFTER WATERMARK
  

-- 全局都按1分钟的延迟输出 (minibatch)
EMIT WITH DELAY '1' MINUTE

-- 窗口结束之前按1分钟延迟输出
EMIT WITH DELAY '1' MINUTE BEFORE WATERMARK
```

# 最大容忍迟到时间

当配置 AFTER 的策略，即表示会接收迟到的数据，窗口的状态就会一直保留以等待迟到数据，那么会等待多久呢？这是一个用户能自定义的参数，即
emit.max.value 状态的超时时间，如果未配置，默认值是1个小时。 ​

# Example

例如，我们有一个一小时的滚动窗口 `tumble_window` 。

```sql
CREATE VIEW tumble_window AS
SELECT id,
       TUMBLE_START(rowtime, INTERVAL '1' HOUR) as start_time,
       COUNT(*)                                 as cnt
FROM source
GROUP BY id, TUMBLE(rowtime, INTERVAL '1' HOUR)
```

默认 `tumble_window` 的输出是需要等到一小时结束才能看到结果，我们希望能尽早能看到窗口的结果（即使是不完整的结果）。例如，我们希望每分钟看到最新的窗口结果：

```sql
INSERT INTO result
SELECT *
FROM tumble_window EMIT WITH DELAY '1' MINUTE BEFORE WATERMARK -- 窗口结束之前，每隔1分钟输出一次更新结果
```

另外，默认 `tumble_window`
会忽略并丢弃窗口结束后到达的数据，而这部分数据对我们来说很重要，希望能统计进最终的结果里。而且我们知道我们的迟到数据不会太多，且迟到时间不会超过1个小时，并且希望收到迟到的数据5秒更新一次结果：

```sql
INSERT INTO result
SELECT *
FROM tumble_window EMIT WITH DELAY '1' MINUTE BEFORE WATERMARK,
WITH DELAY '5' SECOND AFTER WATERMARK -- 窗口结束后，每收到一条数据输出一次更新结果

```

# Delay 的概念

本文的 Delay 指的是用户能忍受的延迟，该延迟是指用户的数据从进入窗口（包括统计，join，over），到看到结果数据（出window）的时间。延迟的计算都是基于窗口时间的。这块和blink略有不同。

# 注意事项

1. 目前 Emit 策略只支持 TUMBLE, HOP 窗口，暂不支持 SESSION 窗口。
1. 目前一个 Job 若有多个输出，多个输出的 Emit 需要定义成一样的策略。
1. 因为语义和blink略有不同，所以可以支持级联窗口，会对所有窗口生效。这块和blink略有不同



