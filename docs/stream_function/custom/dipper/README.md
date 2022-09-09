为了简单，我们未引入任何Dipper的依赖来自定义UDF/UDTF，增加了一些规范，只要按规范书写，一个普通的java bean就可以直接在SQL中引用

# 参数规范

- 如果参数在sql中没有加单引号，且不是数字，会当作字段名，传入是字段值
- 如果参数在sql中没有加单引号，且是数字，会当作数字，传入的是数字值
- 如果参数在sql中加了单引号，会被当作字符串常量，会传入单引号里面的字符值

# Dipper UDF

## 规范要求

- 必须有无参构造函数
- 函数名必须是eval
- 函数的参数类型和返回值，必须是Java的类型
- 如果需要对函数进行初始化，可以加一个open 方法，无参数，无返回值，如果不需要可以不加
- 如果需要对函数进行清理，可以加一个close，无参数，无返回值，如果不需要可以不加
- 参考代码

```java
public class UDFTest {

    public void open() {
        //todo 如果需要，可以添加这个方法，完成资源的初始化
    }

    /**
     * 获取当前时间
     * @return
     */
    public String eval() {
        return DateUtil.getCurrentTimeString();
    }

    /**
     * 获取当前时间
     * @return
     */
    public String eval(String format) {
        return DateUtil.getCurrentTimeString(format);
    }

}
```

## 注册函数

在SQL中，通过create Function注册函数，参考代码如下：

```sql
create Function current_time as 'org.apache.rocketmq.streams.sql.local.runner.UDFTest'
```

- 函数名，可以随便取
- 类名，是实现这个函数的类的全类名

## 发布函数

打包成jar包（所有依赖都需要打进去），放到custom目录即可

## 在SQL中使用

```sql
create Function time_function as 'org.apache.rocketmq.streams.sql.local.runner.UDFTest'

-- 数据源
CREATE TABLE `source_data`
(
    `_index`  VARCHAR,
    `_type`   VARCHAR,
    `_id`     VARCHAR,
    `_score`  VARCHAR,
    `_source` VARCHAR
) with (
      type = 'file',
      filePath = '/tmp/input-log.txt'
      );

CREATE TABLE `data_model`
(
    `_dm_timestamp` varchar,
    `_dm_source`    VARCHAR,
    `_dm_type`      VARCHAR
)
    with (
        type = 'print'
        );

insert into data_model
select time_function() AS _dm_timestamp
    ,_source AS _dm_source
    ,_type AS _dm_type
from
    source_data

```

# Dipper UDTF

## 规范要求

- 必须有无参构造函数
- 函数名必须是eval
- 函数的参数类型，必须是Java的类型
- 函数的返回值，必须是List<Map<String,Object>类型，返回的是拆分后的多行数据，包含多列，每列是一个<key，value>数据，如果没有key的值，请用f0，f1，f2，fn来当作默认的字段名
- 如果需要对函数进行初始化，可以加一个open 方法，无参数，无返回值，如果不需要可以不加
- 如果需要对函数进行清理，可以加一个close，无参数，无返回值，如果不需要可以不加
- 参考代码

```java
public class UDTFTest {

    public void open() {
        //todo 如果需要初始化资源，可以实现这个方法，如果不需要，这个方法可以不写
    }

    /**
     * 获取当前时间
     * @return
     */
    public List<Map<String, Object>> eval(String field, String seperator) {
        String[] values = field.split(seperator);
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int i = 0; i < values.length; i++) {
            Map<String, Object> row = new HashMap<>();
            row.put("f" + i, values[i]);
            rows.add(row);
        }
        return rows;
    }
}

```

## 注册函数

在SQL中，通过create Function注册函数，参考代码如下：

```sql
create Function current_time as 'org.apache.rocketmq.streams.sql.local.runner.UDTFTest'
```

- 函数名，可以随便取
- 类名，是实现这个函数的类的全类名

## 发布函数

打包成jar包（所有依赖都需要打进去），放到custom目录即可

## 在SQL中使用

```sql
create Function time_function as 'org.apache.rocketmq.streams.sql.local.runner.UDTFTest'

-- 数据源
CREATE TABLE `source_data`
(
    `_index`  VARCHAR,
    `_type`   VARCHAR,
    `_id`     VARCHAR,
    `_score`  VARCHAR,
    `_source` VARCHAR
) with (
      type = 'file',
      filePath = '/tmp/input-log.txt'
      );

CREATE TABLE `data_model`
(
    `_dm_timestamp` varchar,
    `_dm_source`    VARCHAR,
    `_dm_type`      VARCHAR,
    `_dm_lable`     VARCHAR
)
    with (
        type = 'print'
        );

insert into data_model
select time_function() AS _dm_timestamp
    ,_source AS _dm_source
    ,_type AS _dm_type
    ,pk_label as _dm_lable
from
    source_data,
    LATERAL TABLE (STRING_SPLIT(_ source, ',')) AS T(pk_label)

```

# Dipper UDAF

请使用Blink的规范定义，dipper暂未开放此能力
