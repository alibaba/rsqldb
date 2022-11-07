# 自定义函数开发

udf（user define function）作为数据处理过程中的额外的补充能力，可以方便的为引入开发者自己个性化的处理逻辑，既利用该特性补充增强内置函数的处理能力，同时也可以复用开发者已有的逻辑实现，降低开发成本。 ​

目前rocketmq-stream支持的udf开发方法共有三种：

## 基于blink udf工具包

rocketmq-streams计算框架全面兼容blink的udf， 既可以直接使用blink的udf 包， 也完全支持基于blink规范开发的自定义UDF，blink udf 的使用和定义可以参考文[https://help.aliyun.com/document_detail/69462.html](https://help.aliyun.com/document_detail/69462.html)。

## 基于rocketmq-stream-script开发

首先需要引入依赖

```xml

<dependency>
    <groupId>org.apache.rocketmq</groupId>
    <artifactId>rocketmq-streams-script</artifactId>
</dependency>
```

实现的udf类需要添加@Function注解以便让rocketmq-stream在进行udf加载时识别扫面，同时在具体方法上添加 @FunctionMethod注解，以表示该方法需要注册为udf。@FunctionMethod 需要配置value属性选配alias属性，value为函数的名称，alias为函数别名，在开发sql过程中引用udf函数名称须严格与这两个配置函数名保持一致。 代码开发示例：

```java
package com;

import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.context.FunctionContext;

@Function
public class ContentExtract {
    @FunctionMethod(value = "extractTest", alias = "extract_test")
    public Object extracttest(IMessage message, FunctionContext context, String config) {
        System.out.println("ContentExtract ========================" + config);

        return "";
    }

}
```

SQL使用实例

```sql
//extract_test为代码中标记@FunctionMethod的value或者alias值
，类为实际类名
CREATE FUNCTION extract_test as 'com.ContentExtract';
```

## 用户已有的函数

需要将注册函数所在类以及依赖的全部jar打包为一个jar，在SQL引用上 函数名称应为实际函数名，类名为实际类名。

上述三种方式，都会生成运行时的jar包， 只需要将jar包放入实时框架的ext目录即可， 实时任务在启动时会自动加载；

