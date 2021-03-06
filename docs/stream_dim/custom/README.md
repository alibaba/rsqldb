# 实现子类

## 引入Maven依赖

```sql
   <dependency>
         <groupId>org.apache.rocketmq</groupId>
         <artifactId>rocketmq-streams-dim</artifactId>
         <version>${project.version}</version>
  </dependency>
```

- version：和使用的dipper版本保持一致

## 实现子类

继承AbstractDim，实现子类，子类有一个抽象方法：

```sql
protected
void loadData2Memory(AbstractMemoryTable tableCompress)
```

这个方法是从数据源加载数据，并把数据存到维表的内存结构(AbstractMemoryTable)中，一行数据是一个Map<String,Object>结构，调用AbstractMemoryTable的addRow(Map<String,Object> row)方法完成维表行数据的注册。

## 例子-文件维表

```sql
  @Override
protected void loadData2Memory(AbstractMemoryTable tableCompress){
        List<String> rows= FileUtil.loadFileLine(filePath);
        for(String row:rows){
            JSONObject jsonObject= JSON.parseObject(row);
            Map<String,Object> values=new HashMap<>();
            for(String key:jsonObject.keySet()){
                values.put(key,jsonObject.getString(key));
            }
        tableCompress.addRow(values);
        }
    }
```

## AbstractMemoryTable介绍

在sql中可以配置isLarge属性，默认是false，如果维表数据量很大，需要设置isLarge=true，系统会切换成MapperByteBufferTable作为维表存储结构，性能会有损失，请谨慎使用。

- AbstractMemoryTable，方法是非线程安全的，如需要多线程并发加载数据，请加synchronized进行同步。
- ByteArrayMemoryTable，这是默认实现，数据量能支撑百万到千万，数据全部在内存，有较高的性能，如果数量不大，建议使用这个数据结构。
    - 每行数据都是基于字节数组存储，没有java中的头部和对齐的开销，内存占用量接近原始数据大小。
    - 有大小限制，最大数据量，不超过2G。每行数据会有（列数+1)*2个字节的开销，拿一个10个字段，1000w的数据举例，假设每行的数据大小100byte，共需要数据量-=(100+11*2)*1000w/1024/1024/1024=1.163g，可以承载。
- MapperByteBufferTable，是基于内存映射的实现方案，可以承载更大的数据体量，数据会存储在磁盘，通过内存映射做热点缓存，会占用系统内存，性能会比ByteArrayMemoryTable有较大下降，但数据体量基本没限制。适合于超大数据规模的维表，但主流数据量不大。

# 在SQL中应用

需要实现AbstractDimParser这个抽象类，实现子类，子类需要加两个标注,：

- ServiceName，里面可以配置value和aliasName，对应sql中with部分的type字段值。
- @AutoService(IDimSQLParser.**class**)，系统会自动加载实现类

```sql
@AutoService(IDimSQLParser.class)
@ServiceName(value = FileDimSQLParser.TYPE, aliasName = "FILE")
public class FileDimSQLParser extends AbstractDimParser{
    public static final String TYPE="file";

    @Override
    protected AbstractDim createDim(Properties properties, MetaData data) {
        String filePath=properties.getProperty("filePath");
        if(StringUtil.isEmpty(filePath)){
            filePath=properties.getProperty("file_path");
        }
        FileDim fileDim=new FileDim();
        fileDim.setFilePath(filePath);
        return fileDim;
    }
}

```

- properties是，sql creat table中的with部分的kv值，上面代码是文件的实现，需要核心字段filePath。
- 在父类中已经处理了type和isLarge的公共属性，子类只需要关注子类加载数据需要的参数即可
- 下面是在sql中的代码，type='file',filePath='/tmp/dim_data_model_extractor.txt'

```sql
CREATE TABLE `extractor_config`
(
    data_source varchar,
    id          varchar,
    extractor   varchar
) with (
      type = 'file' --定义维表的变化周期，表明该表是一张会变化的表。
      ,filePath = '/tmp/dim_data_model_extractor.txt'
      )
;
```

只要把jar包放到dipper可以加载的位置（如ext或lib）中，系统会自动识别和加载  
