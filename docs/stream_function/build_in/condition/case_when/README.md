# 语法

```sql
CASE WHEN a THEN b [WHEN c THEN d]* [ELSE e]
END
```

# 功能描述

如果a为TRUE,则返回b；如果c为TRUE，则返回d；否则返回e 。

# 测试数据

| device_type(VARCHAR) |
| --- |
| android |
| ios |
| win |

# 测试案例

```sql
create table T1
(
    device_type varchar

) with (
      type = 'tt',
      topic = 'topic1',
      accessId = 'xxxx',
      accessKey = 'xxxxxxx'
      );
create table tt_output
(
    os     varchar,
    length int
) with (
      type = 'print'
      );
INSERT INTO tt_output
SELECT os,
       CHAR_LENGTH(os)
from (
         SELECT case
                    when device_type = 'android'
                        then 'android'
                    else 'ios'
                    end as os
         FROM T1
     );
```

# 测试结果

| os(VARCHAR) | length(INT) |
| --- | --- |
| android | 7 |
| ios | 3 |
| ios | 3 |


