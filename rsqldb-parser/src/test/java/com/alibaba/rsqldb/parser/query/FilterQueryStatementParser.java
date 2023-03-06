 /*
  * Copyright 1999-2023 Alibaba Group Holding Ltd.
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  *      http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
 package com.alibaba.rsqldb.parser.query;

 import com.alibaba.rsqldb.parser.SerDer;
 import com.alibaba.rsqldb.parser.model.Calculator;
 import com.alibaba.rsqldb.parser.model.Field;
 import com.alibaba.rsqldb.parser.model.Operator;
 import com.alibaba.rsqldb.parser.model.WildcardType;
 import com.alibaba.rsqldb.parser.model.baseType.BooleanType;
 import com.alibaba.rsqldb.parser.model.baseType.Literal;
 import com.alibaba.rsqldb.parser.model.baseType.MultiLiteral;
 import com.alibaba.rsqldb.parser.model.baseType.NumberType;
 import com.alibaba.rsqldb.parser.model.baseType.StringType;
 import com.alibaba.rsqldb.parser.model.expression.AndExpression;
 import com.alibaba.rsqldb.parser.model.expression.Expression;
 import com.alibaba.rsqldb.parser.model.expression.MultiValueExpression;
 import com.alibaba.rsqldb.parser.model.expression.OrExpression;
 import com.alibaba.rsqldb.parser.model.expression.RangeValueExpression;
 import com.alibaba.rsqldb.parser.model.expression.SingleValueExpression;
 import com.alibaba.rsqldb.parser.model.expression.WildcardExpression;
 import com.alibaba.rsqldb.parser.model.statement.query.FilterQueryStatement;
 import org.junit.Test;

 import java.util.List;
 import java.util.Map;

 import static org.junit.Assert.assertEquals;
 import static org.junit.Assert.assertFalse;
 import static org.junit.Assert.assertNull;
 import static org.junit.Assert.assertSame;
 import static org.junit.Assert.assertTrue;

 public class FilterQueryStatementParser extends SerDer {
     @Test
     public void query0() throws Throwable {
         String sql = "select field_1\n" +
                 "     , field_2\n" +
                 "     , field_3\n" +
                 "     , field_4\n" +
                 "from rocketmq_source where field_1=`1123`;";

         FilterQueryStatement filterQueryStatement = super.parser(sql, FilterQueryStatement.class);

         String tableName = filterQueryStatement.getTableName();
         assertEquals("rocketmq_source", tableName);

         Map<Field, Calculator> fieldAndCalculator = filterQueryStatement.getSelectFieldAndCalculator();
         assertEquals(4, fieldAndCalculator.size());

         Expression filter = filterQueryStatement.getFilter();
         assertTrue(filter instanceof SingleValueExpression);

         SingleValueExpression singleValueExpression = (SingleValueExpression) filter;
         Operator operator = singleValueExpression.getOperator();
         Field field = singleValueExpression.getField();
         Literal<?> value = singleValueExpression.getValue();

         assertSame(operator, Operator.EQUAL);
         assertEquals("field_1", field.getFieldName());
         assertTrue(value instanceof StringType);

         StringType stringType = (StringType) value;
         assertEquals("1123", stringType.result());
     }

     @Test
     public void query2() throws Throwable {
         String sql = "select field_1\n" +
                 "     , field_2\n" +
                 "     , field_3\n" +
                 "     , field_4\n" +
                 "from rocketmq_source where field_1=23;";

         FilterQueryStatement filterQueryStatement = super.parser(sql, FilterQueryStatement.class);

         String tableName = filterQueryStatement.getTableName();
         assertEquals("rocketmq_source", tableName);

         Map<Field, Calculator> fieldAndCalculator = filterQueryStatement.getSelectFieldAndCalculator();
         assertEquals(4, fieldAndCalculator.size());

         Expression filter = filterQueryStatement.getFilter();
         assertTrue(filter instanceof SingleValueExpression);

         SingleValueExpression singleValueExpression = (SingleValueExpression) filter;
         Operator operator = singleValueExpression.getOperator();
         Field field = singleValueExpression.getField();
         Literal<?> value = singleValueExpression.getValue();

         assertSame(operator, Operator.EQUAL);
         assertEquals("field_1", field.getFieldName());
         assertTrue(value instanceof NumberType);

         NumberType stringType = (NumberType) value;
         assertEquals(23, stringType.result());
     }

     @Test
     public void query21() throws Throwable {
         String sql = "select field_1\n" +
                 "     , field_2\n" +
                 "     , field_3\n" +
                 "     , field_4\n" +
                 "from rocketmq_source where rocketmq_source.field_1=23;";

         FilterQueryStatement filterQueryStatement = super.parser(sql, FilterQueryStatement.class);

         String tableName = filterQueryStatement.getTableName();
         assertEquals("rocketmq_source", tableName);

         Map<Field, Calculator> fieldAndCalculator = filterQueryStatement.getSelectFieldAndCalculator();
         assertEquals(4, fieldAndCalculator.size());

         Expression filter = filterQueryStatement.getFilter();
         assertTrue(filter instanceof SingleValueExpression);

         SingleValueExpression singleValueExpression = (SingleValueExpression) filter;
         Operator operator = singleValueExpression.getOperator();
         Field field = singleValueExpression.getField();
         Literal<?> value = singleValueExpression.getValue();

         assertSame(operator, Operator.EQUAL);
         assertEquals("rocketmq_source", field.getTableName());
         assertEquals("field_1", field.getFieldName());
         assertTrue(value instanceof NumberType);

         NumberType stringType = (NumberType) value;
         assertEquals(23, stringType.result());
     }


     @Test
     public void query3() throws Throwable {
         String sql = "select field_1\n" +
                 "     , field_2\n" +
                 "     , field_3\n" +
                 "     , field_4\n" +
                 "from rocketmq_source where field_1='234';";

         FilterQueryStatement filterQueryStatement = super.parser(sql, FilterQueryStatement.class);

         String tableName = filterQueryStatement.getTableName();
         assertEquals("rocketmq_source", tableName);

         Map<Field, Calculator> fieldAndCalculator = filterQueryStatement.getSelectFieldAndCalculator();
         assertEquals(4, fieldAndCalculator.size());

         Expression filter = filterQueryStatement.getFilter();
         assertTrue(filter instanceof SingleValueExpression);

         SingleValueExpression singleValueExpression = (SingleValueExpression) filter;
         Operator operator = singleValueExpression.getOperator();
         Field field = singleValueExpression.getField();
         Literal<?> value = singleValueExpression.getValue();

         assertSame(operator, Operator.EQUAL);
         assertEquals("field_1", field.getFieldName());
         assertTrue(value instanceof StringType);

         StringType stringType = (StringType) value;
         assertEquals("234", stringType.result());
     }

     @Test
     public void query4() throws Throwable {
         String sql = "select field_1\n" +
                 "     , field_2\n" +
                 "     , field_3\n" +
                 "     , field_4\n" +
                 "from rocketmq_source where field_1=\"234\";";

         FilterQueryStatement filterQueryStatement = super.parser(sql, FilterQueryStatement.class);

         String tableName = filterQueryStatement.getTableName();
         assertEquals("rocketmq_source", tableName);

         Map<Field, Calculator> fieldAndCalculator = filterQueryStatement.getSelectFieldAndCalculator();
         assertEquals(4, fieldAndCalculator.size());

         Expression filter = filterQueryStatement.getFilter();
         assertTrue(filter instanceof SingleValueExpression);

         SingleValueExpression singleValueExpression = (SingleValueExpression) filter;
         Operator operator = singleValueExpression.getOperator();
         Field field = singleValueExpression.getField();
         Literal<?> value = singleValueExpression.getValue();

         assertSame(operator, Operator.EQUAL);
         assertEquals("field_1", field.getFieldName());
         assertTrue(value instanceof StringType);

         StringType stringType = (StringType) value;
         assertEquals("234", stringType.result());
     }


     @Test
     public void query10() throws Throwable {
         String sql = "select field_1\n" +
                 "     , field_2\n" +
                 "     , field_3\n" +
                 "     , field_4\n" +
                 "from rocketmq_source where field_1 is null";

         FilterQueryStatement filterQueryStatement = super.parser(sql, FilterQueryStatement.class);
         String tableName = filterQueryStatement.getTableName();
         assertEquals("rocketmq_source", tableName);

         Expression filter = filterQueryStatement.getFilter();
         assertTrue(filter instanceof SingleValueExpression);
         SingleValueExpression valueExpression = (SingleValueExpression) filter;

         assertNull(valueExpression.getValue());
         assertEquals(valueExpression.getField().getFieldName(), "field_1");

         assertEquals(valueExpression.getOperator(), Operator.EQUAL);
     }

     @Test
     public void query11() throws Throwable {
         String sql = "select field_1\n" +
                 "     , field_2\n" +
                 "     , field_3\n" +
                 "     , field_4\n" +
                 "from rocketmq_source where field_1 between 1 and 10;";

         FilterQueryStatement filterQueryStatement = super.parser(sql, FilterQueryStatement.class);
         String tableName = filterQueryStatement.getTableName();
         assertEquals("rocketmq_source", tableName);

         Expression filter = filterQueryStatement.getFilter();
         assertTrue(filter instanceof RangeValueExpression);
         RangeValueExpression valueExpression = (RangeValueExpression) filter;

         assertEquals(1, valueExpression.getLow(), 0);
         assertEquals(10, valueExpression.getHigh(), 0);
         assertEquals(valueExpression.getField().getFieldName(), "field_1");

         assertEquals(valueExpression.getOperator(), Operator.BETWEEN_AND);
     }

     @Test
     public void query12() throws Throwable {
         String sql = "select field_1\n" +
                 "     , field_2\n" +
                 "     , field_3\n" +
                 "     , field_4\n" +
                 "from rocketmq_source where field_1 in(`1q2`, \"w2q\", 122, \"123\");";

         FilterQueryStatement filterQueryStatement = super.parser(sql, FilterQueryStatement.class);
         String tableName = filterQueryStatement.getTableName();
         assertEquals("rocketmq_source", tableName);

         Expression filter = filterQueryStatement.getFilter();

         assertTrue(filter instanceof MultiValueExpression);
         MultiValueExpression valueExpression = (MultiValueExpression) filter;

         Operator operator = valueExpression.getOperator();
         String fieldName = valueExpression.getField().getFieldName();
         MultiLiteral values = valueExpression.getValues();
         List<Literal<?>> result = values.result();

         assertEquals(Operator.IN, operator);
         assertEquals("field_1", fieldName);

         assertEquals(4, result.size());
     }

     @Test
     public void query13() throws Throwable {
         String sql = "select field_1\n" +
                 "     , field_2\n" +
                 "     , field_3\n" +
                 "     , field_4\n" +
                 "from rocketmq_source where field_1 in(`1q2`, \"w2q\", 122, \"123\") and field_2=2;";

         FilterQueryStatement filterQueryStatement = super.parser(sql, FilterQueryStatement.class);
         String tableName = filterQueryStatement.getTableName();
         assertEquals("rocketmq_source", tableName);

         Expression filter = filterQueryStatement.getFilter();

         assertTrue(filter instanceof AndExpression);
         AndExpression andExpression = (AndExpression) filter;

         Expression leftExpression = andExpression.getLeftExpression();
         Expression rightExpression = andExpression.getRightExpression();

         {
             assertTrue(leftExpression instanceof MultiValueExpression);
             MultiValueExpression valueExpression = (MultiValueExpression) leftExpression;

             Operator operator = valueExpression.getOperator();
             String fieldName = valueExpression.getField().getFieldName();
             MultiLiteral values = valueExpression.getValues();
             List<Literal<?>> result = values.result();
             assertEquals(Operator.IN, operator);
             assertEquals("field_1", fieldName);
             assertEquals(4, result.size());
         }

         {
             assertTrue(rightExpression instanceof SingleValueExpression);
             SingleValueExpression singleValueExpression = (SingleValueExpression) rightExpression;
             Operator operator = singleValueExpression.getOperator();
             Field field = singleValueExpression.getField();
             Literal<?> value = singleValueExpression.getValue();

             assertSame(operator, Operator.EQUAL);
             assertEquals("field_2", field.getFieldName());
             assertTrue(value instanceof NumberType);

             NumberType stringType = (NumberType) value;
             assertEquals(2, stringType.result());
         }
     }

     @Test
     public void query14() throws Throwable {
         String sql = "select field_1\n" +
                 "     , field_2\n" +
                 "     , field_3\n" +
                 "     , field_4\n" +
                 "from rocketmq_source where field_1 in(`1q2`, \"w2q\", 122, \"123\") or field_2=2;";

         FilterQueryStatement filterQueryStatement = super.parser(sql, FilterQueryStatement.class);
         String tableName = filterQueryStatement.getTableName();
         assertEquals("rocketmq_source", tableName);

         Expression filter = filterQueryStatement.getFilter();

         assertTrue(filter instanceof OrExpression);
         OrExpression orExpression = (OrExpression) filter;

         Expression leftExpression = orExpression.getLeftExpression();
         Expression rightExpression = orExpression.getRightExpression();

         {
             assertTrue(leftExpression instanceof MultiValueExpression);
             MultiValueExpression valueExpression = (MultiValueExpression) leftExpression;

             Operator operator = valueExpression.getOperator();
             String fieldName = valueExpression.getField().getFieldName();
             MultiLiteral values = valueExpression.getValues();
             List<Literal<?>> result = values.result();
             assertEquals(Operator.IN, operator);
             assertEquals("field_1", fieldName);
             assertEquals(4, result.size());
         }

         {
             assertTrue(rightExpression instanceof SingleValueExpression);
             SingleValueExpression singleValueExpression = (SingleValueExpression) rightExpression;
             Operator operator = singleValueExpression.getOperator();
             Field field = singleValueExpression.getField();
             Literal<?> value = singleValueExpression.getValue();

             assertSame(operator, Operator.EQUAL);
             assertEquals("field_2", field.getFieldName());
             assertTrue(value instanceof NumberType);

             NumberType stringType = (NumberType) value;
             assertEquals(2, stringType.result());
         }
     }

     @Test
     public void query15() throws Throwable {
         String sql = "select field_1\n" +
                 "     , field_2\n" +
                 "     , field_3\n" +
                 "     , field_4\n" +
                 "from rocketmq_source where field_1<10 or field_2=2 and field_3=true";

         FilterQueryStatement filterQueryStatement = super.parser(sql, FilterQueryStatement.class);
         String tableName = filterQueryStatement.getTableName();
         assertEquals("rocketmq_source", tableName);

         Expression filter = filterQueryStatement.getFilter();

         //same with last joint
         assertTrue(filter instanceof AndExpression);

         AndExpression andExpression = (AndExpression) filter;

         Expression leftExpression = andExpression.getLeftExpression();
         Expression rightExpression = andExpression.getRightExpression();

         assertTrue(leftExpression instanceof OrExpression);
         assertTrue(rightExpression instanceof SingleValueExpression);

         {
             OrExpression orExpression = (OrExpression) leftExpression;
             Expression leftLeftExpression = orExpression.getLeftExpression();
             Expression leftRightExpression = orExpression.getRightExpression();

             assertTrue(leftLeftExpression instanceof SingleValueExpression);
             assertTrue(leftRightExpression instanceof SingleValueExpression);

             SingleValueExpression leftLeftSingleExpression = (SingleValueExpression) leftLeftExpression;
             SingleValueExpression leftRightSingleExpression = (SingleValueExpression) leftRightExpression;

             {
                 Operator operator = leftLeftSingleExpression.getOperator();
                 String fieldName = leftLeftSingleExpression.getField().getFieldName();
                 Literal<?> value = leftLeftSingleExpression.getValue();

                 assertSame(Operator.LESS, operator);
                 assertEquals("field_1", fieldName);
                 assertTrue(value instanceof NumberType);
                 NumberType numberType = (NumberType) value;
                 assertEquals(10, numberType.getNumber());
             }

             {
                 Operator operator = leftRightSingleExpression.getOperator();
                 String fieldName = leftRightSingleExpression.getField().getFieldName();
                 Literal<?> value = leftRightSingleExpression.getValue();

                 assertSame(Operator.EQUAL, operator);
                 assertEquals("field_2", fieldName);
                 assertTrue(value instanceof NumberType);
                 NumberType numberType = (NumberType) value;
                 assertEquals(2, numberType.getNumber());
             }
         }

         {
             SingleValueExpression singleValueExpression = (SingleValueExpression) rightExpression;
             Operator operator = singleValueExpression.getOperator();
             String fieldName = singleValueExpression.getField().getFieldName();
             Literal<?> value = singleValueExpression.getValue();

             assertSame(Operator.EQUAL, operator);
             assertEquals("field_3", fieldName);
             assertTrue(value instanceof BooleanType);
             BooleanType booleanType = (BooleanType) value;
             assertTrue(booleanType.result());
         }
     }


     @Test
     public void query20() throws Throwable {
         String sql = "select field_1\n" +
                 "     , field_2\n" +
                 "     , field_3\n" +
                 "     , field_4\n" +
                 "from rocketmq_source where field_1 like '%topic-';";
         FilterQueryStatement filterQueryStatement = super.parser(sql, FilterQueryStatement.class);
         assertEquals("rocketmq_source", filterQueryStatement.getTableName());

         Expression filter = filterQueryStatement.getFilter();

         assertTrue(filter instanceof WildcardExpression);

         WildcardExpression wildcardExpression = (WildcardExpression) filter;

         assertEquals("topic-", wildcardExpression.getTarget());
         assertEquals(WildcardType.PREFIX_LIKE, wildcardExpression.getType());
         assertFalse(wildcardExpression.isCaseSensitive());
         assertEquals(Operator.LIKE, wildcardExpression.getOperator());
         assertEquals("field_1",wildcardExpression.getField().getFieldName());
     }

     @Test
     public void query22() throws Throwable {
         String sql = "select field_1\n" +
                 "     , field_2\n" +
                 "     , field_3\n" +
                 "     , field_4\n" +
                 "from rocketmq_source where field_1 like '%topic-' and field_2>1;";
         FilterQueryStatement filterQueryStatement = super.parser(sql, FilterQueryStatement.class);
         assertEquals("rocketmq_source", filterQueryStatement.getTableName());

         Expression filter = filterQueryStatement.getFilter();

         assertTrue(filter instanceof AndExpression);

         AndExpression andExpression = (AndExpression) filter;

         assertTrue(andExpression.getLeftExpression() instanceof WildcardExpression);
         assertTrue(andExpression.getRightExpression() instanceof SingleValueExpression);
     }
 }
