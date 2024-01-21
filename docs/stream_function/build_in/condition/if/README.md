# 语法

```sql
T
IF(BOOLEAN testCondition, T valueTrue, T valueFalseOrNull)
```

# 入参

- testCondition BOOLEAN 类型
- valueTrue, valueFalseOrNull
    - 参数类型均为数值类型，或
    - 参数类型需要一致。

# 功能描述

以第一个参数的布尔值为判断标准，若为 true，则返回第二个参数，若为 false，则返回第三个参数。 第一个参数为 null 时看作
false，剩下参数为 null 按照正常语义运行。

# 示例

- 测试数据

| int1(INT) | int2(INT) | int3(INT) | double1(DOUBLE) | str1(VARCHAR) | str2(VARCHAR) |
|-----------|-----------|-----------|-----------------|---------------|---------------|
| 1         | 2         | 1         | 2.0             | Jack          | Harry         |
| 1         | 2         | 1         | null            | Jack          | null          |
| 1         | 2         | null      | 2.0             | null          | Harry         |

- 测试案例

```sql
SELECT IF(int1 < int2, str1, str2)    as v1,
       IF(int1 < int2, int3, double1) as v2
FROM T1
```

- 测试结果

| v1(VARCHAR) | v2(DOUBLE) |
|-------------|------------|
| Jack        | 1.0        |
| Jack        | 1.0        |
| null        | null       |





