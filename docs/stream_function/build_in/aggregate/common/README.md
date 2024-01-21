| 函数    | 语法              | 描述                                                              | 案例                                      |
|-------|-----------------|-----------------------------------------------------------------|-----------------------------------------|
| COUNT | BIGINT COUNT(A) | 返回行数，输入参数无限制                                                    | SELECT COUNT(var1)  as aa <br> FROM T1; |
| AVG   | DOUBLE AVG(A)   | 返回平均数，支持数值类型( TINYINT SMALLINT INT BIGINT FLOAT DECIMAL DOUBLE) | SELECT AVG(var1)  as aa <br> FROM T1;   |
| MAX   | MAX(A)          | 入参A是什么类型就返回什么类型，支持数字，时间和字符串类型                                   | 返回所有输入值的值的最大值                           |
| SUM   | SUM(A)          | 返回所有输入值之间的数值之和， <br> 支持所有数字类型                                   | SELECT sum(var1)  as aa <br> FROM T1;   |
| MIN   | MIN(A)          | 返回所有输入值的值的最小值，支持数字，时间和字符串类型                                     | ，支持数字，时间和字符串类型                          |