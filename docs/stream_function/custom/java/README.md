为了复用，可以直接复用已有的java代码，如何把一个已有的java代码发布成UDF，请参考下面的说明

# 参数规范

- 如果参数在sql中没有加单引号，且不是数字，会当作字段名，传入是字段值
- 如果参数在sql中没有加单引号，且是数字，会当作数字，传入的是数字值
- 如果参数在sql中加了单引号，会被当作字符串常量，会传入单引号里面的字符值

# UDF

## 规范要求

- 必须有无参构造函数
- 如果需要对函数进行初始化，可以加一个open 方法，无参数，无返回值，如果不需要就可以不加。其他函数不会被执行。
- 参考代码

```java
public class JavaUDF {

    /**
     * 获取当前时间
     * @return
     */
    public String getCurrentTime() {
        return DateUtil.getCurrentTimeString();
    }
}

```

## 注册函数

在SQL中，通过create Function注册函数，参考代码如下：

```sql
create Function getCurrentTime as 'org.apache.rocketmq.streams.sql.local.runner.UDFTest'
```

- 函数名，必须是实现类的方法名
- 类名，是实现这个函数的类的全类名

## 发布函数

打包成jar包（所有依赖都需要打进去），放到custom目录即可

## 在SQL中使用

```sql
create Function getCurrentTime as 'org.apache.rocketmq.streams.sql.local.runner.UDFTest'

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
) with (
    type = 'print'
    );

insert into data_model
select getCurrentTime() AS _dm_timestamp
    ,_source AS _dm_source
    ,_type AS _dm_type
from
    source_data

```

# UDTF

现有java类，暂不支持UDTF的直接复用，请参照[dipper udtf规范](https://yuque.antfin-inc.com/chris.yxd/ye786c/kk0475)改造
