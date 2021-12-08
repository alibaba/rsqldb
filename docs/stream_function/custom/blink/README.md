只要blink udf/udtf/udaf未引用blink上下文信息，可以兼容，但也有可能测试不到位的，如发现有不兼容的可以找我反馈

# Blink UDF

用户定义的标量函数将零个，一个或多个标量值映射到一个新的标量值。

## 编写业务逻辑代码

以java为例：

```java
package com.hjc.test.blink.sql.udx;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

public class StringLengthUdf extends ScalarFunction {
    // 可选， open方法可以不写
    // 需要import org.apache.flink.table.functions.FunctionContext;
    @Override
    public void open(FunctionContext context) {
    }

    public long eval(String a) {
        return a == null ? 0 : a.length();
    }

    public long eval(String b, String c) {
        return eval(b) + eval(c);
    }

    //可选，close方法可以不写
    @Override
    public void close() {
    }
}
```

## 上线

- 找到你指定的class编写SQL语句后，点击上线、进入运维页面点击启动就可以运行了。

```sql

-- udf str.length()
CREATE FUNCTION stringLengthUdf AS 'com.hjc.test.blink.sql.udx.StringLengthUdf';

create table sls_stream
(
    a int,
    b int,
    c varchar
) with (
      type = 'sls',
      endPoint = '',
      accessKeyId = '',
      accessKeySecret = '',
      startTime = '2017-07-04 00:00:00',
      project = '',
      logStore = '',
      consumerGroup = 'consumerGroupTest1'
      );

create table rds_output
(
    id      int,
    len     bigint,
    content VARCHAR
) with (
      type = 'rds',
      url = '',
      tableName = '',
      userName = '',
      password = ''
      );

insert into rds_output
select a,
       stringLengthUdf(c),
       c as content
from sls_stream
```

# Blink UDTF

与用户定义的标量函数类似，用户定义的表函数将零个，一个或多个标量值作为输入参数。然而，与标量函数相反，它可以返回任意数量的行作为输出，而不是单个值。返回的行可以由一个或多个列组成。


> 注意： UDTF 使用 `collect(T)` 函数收集返回的多行数据，但注意 `collect(T)` 不能并发调用，否则会有异常。

## 编写业务逻辑代码

以java为例：

```java
package com.hjc.test.blink.sql.udx;

import org.apache.flink.table.functions.TableFunction;

public class SplitUdtf extends TableFunction<String> {

　　// 可选， open方法可以不写
    // 需要import org.apache.flink.table.functions.FunctionContext;

    @Override
    public void open(FunctionContext context) {
        // ... ...
  　}

    public void eval(String str) {
        String[] split = str.split("\\|");
        for (String s : split) {
            collect(s);
        }
    }

    // 可选，close方法可以不写
    @Override
    public void close() {
        // ... ...
  　}
}
```

## 多行返回

UDTF可以将一行的数据转为多行返回。

## 多列返回

UDTF不仅可以做到一行转多行，也可以做到一列转多列。如果用户希望UDTF返回多列，只需要将返回值声明成Tuple或Row即可。Blink支持 Tuple1 ~ Tuple25 ，分别可以定义1个字段到25个字段。

如下示例展示了用Tuple3来返回三个字段的UDTF：

```java
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.functions.TableFunction;

// 使用Tuple作为返回值，一定要显式声明Tuple的泛型类型, 如此例的 String, Long, Integer
public class ParseUdtf extends TableFunction<Tuple3<String, Long, Integer>> {

    public void eval(String str) {
        String[] split = str.split(",");
        // 仅作示例，实际业务需要添加更多的校验逻辑
        String first = split[0];
        long second = Long.parseLong(split[1]);
        int third = Integer.parseInt(split[2]);
        Tuple3<String, Long, Integer> tuple3 = Tuple3.of(first, second, third);
        collect(tuple3);
    }
}
```

如果使用Row来实现返回三个字段的UDTF：

```java
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.DataTypes;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

public class ParseUdtf extends TableFunction<Row> {

    public void eval(String str) {
        String[] split = str.split(",");
        String first = split[0];
        long second = Long.parseLong(split[1]);
        int third = Integer.parseInt(split[2]);
        Row row = new Row(3);
        row.setField(0, first);
        row.setField(1, second);
        row.setField(2, third);
        collect(row);
    }

    @Override
    // 如果返回值是Row，就必须重载实现这个方法，显式地告诉系统返回的字段类型
    //对于blink 2.X版本，请使用以下方法实现重载
    public DataType getResultType(Object[] arguments, Class[] argTypes) {
        return DataTypes.createRowType(DataTypes.STRING, DataTypes.LONG, DataTypes.INT);
    }

    //对于blink 1.X版本，请使用以下方法实现重载
    public TypeInformation<Row> getResultType() {
        return Types.ROW(Types.STRING, Types.LONG, Types.INT);
    }
}
```

使用Tuple的限制是，字段值不能为null，且最多只能有25个字段。Row的字段值可以是null，但是使用Row的话，必须重载实现`getResultType`方法以告诉系统每个字段的类型。

## SQL 语法

UDTF 支持cross join 和 left join，在使用udtf的时候需要带上 `LATERAL`和`TABLE`两个关键字，以上述的`ParseUdtf`为示例，首先注册一个function名字。

```sql
CREATE FUNCTION parseUdtf AS 'com.alibaba.blink.sql.udtf.ParseUdtf';
```

CROSS JOIN：左表的每一行数据都会关联上udtf产出的每一行数据，如果udtf不产出任何数据，那么这一行不会输出。

```sql
select S.id, S.content, T.a, T.b, T.c
from input_stream as S,
     LATERAL TABLE(parseUdtf(content)) as T(a, b, c);
```

LEFT JOIN: 左表的每一行数据都会关联上udtf产出的每一行数据，如果udtf不产出任何数据，那么这一行的UDTF的字段会用null值填充。注意：LEFT JOIN UDTF 必须要在后面跟上 `ON TRUE`。

```sql
select S.id, S.content, T.a, T.b, T.c
from input_stream as S
         LEFT JOIN LATERAL TABLE(parseUdtf(content)) as T(a, b, c)
ON TRUE;
```

# Blink UDAF

用户自定义聚合函数，将多条记录聚合成一条值。

## UDAF抽象类内部的方法介绍

先介绍一下AggregateFunction的几个核心接口方法（⚠️：虽然UDAF的可以用java或者scala实现，但是建议用户使用java，主要原因是scala的数据类型有时会造成不必要的性能overhead）

```java
/*
 * @param <T>  UDAF的输出结果的类型
 * @param <ACC> UDAF的accumulator的类型。accumulator是UDAF计算中用来存放计算中间结果的数
 * 据类型, 用户需要根据需要，自己设计每个UDAF的accumulator。例如，最简单的count UDAF，
 * accumulator就可以是一个只包含一个
 */
public abstract class AggregateFunction<T, ACC> extends UserDefinedFunction {
    /*
     * 初始化AggregateFunction的accumulator，
     * 系统在第一个做aggregate计算之前调用一次这个方法
     */
    public ACC createAccumulator()；

    /*
     * 系统在每次aggregate计算完成后调用这个方法
     */
    public T getValue(ACC accumulator)；
}
```

createAccumulator和getValue的输入输出是确定的，所以可以定义在AggregateFunction抽象类内。除了这两个方法，要设计一个最基本的UDAF，还必须要有一个accumulate方法。

```java
/*
 * 用户需要实现一个accumulate方法，来描述如何将用户的输入的数据计算，并更新到accumulator中。
 * accumulate方法的第一个参数必须是使用AggregateFunction的ACC类型的accumulator。在系统运行
 * 过程中，底层runtime代码会把历史状态accumulator，和用户指定的上游数据（支持任意数量，任意
 * 类型的数据）做为参数一起发送给accumulate计算。
 */
public void accumulate(ACC accumulator,...[用户指定的输入参数]...);
```

createAccumulator，getValue 和 accumulate三个方法一起，就能设计出一个最基本的UDAF。但是流计算有一些特殊的场景需要用户提供retract和merge两个方法才能完成。

```java
/*
 * 在流计算的场景里面很多时候的计算都是对无限流的一个提前的观测值（early firing）。既然有
 * early firing，就会有对发出的结果的修改，这个操作叫做撤回（retract），SQL翻译优化器会帮助
 * 用户自动的判断哪些情况下会产生撤回的数据，哪些操作需要处理带有撤回标记的数据。如何处理撤回
 * 的数据，需要用户自己实现一个retract方法。retract方法是accumulate方法的逆操作。例如count
 * UDAF，在accumulate的时候每来一条数据要加一，在retract的时候就是要减一。类似于accumulate
 * 方法，retract方法的第一个参数必须是使用AggregateFunction的ACC类型的accumulator。在系统
 * 运行过程中，底层runtime代码会把历史状态accumulator，和用户指定的上游数据（支持任意数量，
 * 任意类型的数据）一起发送给retract计算。
 */
public void retract(ACC accumulator,...[用户指定的输入参数]...);

/*
 * merge方法在批计算中被广泛的用到，在流计算中也有些场景需要merge，例如sesession window。
 * 由于流计算具有out of order的特性，后到的数据往往有可能位于两个原本分开的session中间，这
 * 样就把两个session合为一个session。这种场景出现的时候，我们就需要一个merge方法把几个
 * accumulator合为一个accumulator。merge方法的第一个参数必须是使用AggregateFunction的ACC
 * 类型的accumulator，而且这第一个accumulator是merge方法完成之后状态所存放的地方。merge方法
 * 的第二个参数是一个ACC type的accumulator遍历迭代器，里面有可能有一个或者多个accumulator。
 */
public void merge(ACC accumulator,Iterable<ACC> its);
```

## 编写业务逻辑代码

以java为例：

```java

import org.apache.flink.table.functions.AggregateFunction;

public class CountUdaf extends AggregateFunction<Long, CountUdaf.CountAccum> {
    //定义存放count udaf的状态的accumulator数据结构
    public static class CountAccum {
        public long total;
    }

    //初始化count udaf的accumulator
    public CountAccum createAccumulator() {
        CountAccum acc = new CountAccum();
        acc.total = 0;
        return acc;
    }

    //getValue提供了如何通过存放状态的accumulator计算count UDAF的结果的方法
    public Long getValue(CountAccum accumulator) {
        return accumulator.total;
    }

    //accumulate提供了如何根据输入的数据更新count UDAF存放状态的accumulator
    public void accumulate(CountAccum accumulator, Object iValue) {
        accumulator.total++;
    }

    public void merge(CountAccum accumulator, Iterable<CountAccum> its) {
        for (CountAccum other : its) {
            accumulator.total += other.total;
        }
    }
}
```

注: AggregateFunction的子类支持 open 和 close 方法，作为可选方法，可参考UDF或UDTF的写法。
