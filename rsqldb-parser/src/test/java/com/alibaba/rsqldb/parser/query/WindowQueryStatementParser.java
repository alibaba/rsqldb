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
import com.alibaba.rsqldb.parser.model.baseType.Literal;
import com.alibaba.rsqldb.parser.model.baseType.NumberType;
import com.alibaba.rsqldb.parser.model.expression.Expression;
import com.alibaba.rsqldb.parser.model.expression.SingleValueCalcuExpression;
import com.alibaba.rsqldb.parser.model.expression.SingleValueExpression;
import com.alibaba.rsqldb.parser.model.statement.query.WindowInfoInSQL;
import com.alibaba.rsqldb.parser.model.statement.query.WindowQueryStatement;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class WindowQueryStatementParser extends SerDer {
    @Test
    public void test0() throws Throwable {
        String sql = "SELECT\n" +
                "    TUMBLE_START(ts, INTERVAL '1' MINUTE)       AS  window_start,\n" +
                "    TUMBLE_END(ts, INTERVAL '1' MINUTE)         AS  window_end,\n" +
                "    username                                    AS  username,\n" +
                "    age                                         AS  age,\n" +
                "    COUNT(click_url)                            AS  clicks\n" +
                "FROM window_test\n" +
                "WHERE age != 10 " +
                "GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE), username " +
                "HAVING COUNT(click_url) < 100";

        WindowQueryStatement windowQueryStatement = parser(sql, WindowQueryStatement.class);
        assertEquals("window_test", windowQueryStatement.getTableName());

        List<Field> groupByField = windowQueryStatement.getGroupByField();
        assertEquals(1, groupByField.size());
        assertEquals("username", groupByField.get(0).getFieldName());

        Map<Field, Calculator> selectFieldAndCalculator = windowQueryStatement.getSelectFieldAndCalculator();
        for (Field field : selectFieldAndCalculator.keySet()) {
            if (field.getAsFieldName().equals("window_start")) {
                Calculator calculator = selectFieldAndCalculator.get(field);
                assertSame(Calculator.WINDOW_START, calculator);
            } else if (field.getAsFieldName().equals("window_end")) {
                Calculator calculator = selectFieldAndCalculator.get(field);
                assertSame(Calculator.WINDOW_END, calculator);
            }
        }

        Expression whereExpression = windowQueryStatement.getWhereExpression();
        Expression havingExpression = windowQueryStatement.getHavingExpression();

        assertTrue(whereExpression instanceof SingleValueExpression);
        assertTrue(havingExpression instanceof SingleValueCalcuExpression);

        {
            SingleValueExpression singleValueExpression = (SingleValueExpression) whereExpression;
            Literal<?> value = singleValueExpression.getValue();
            Operator operator = singleValueExpression.getOperator();
            Field field = singleValueExpression.getField();

            assertEquals("age", field.getFieldName());
            assertNull(field.getTableName());
            assertNull(field.getAsFieldName());
            assertSame(Operator.NOT_EQUAL, operator);

            assertTrue(value instanceof NumberType);
            assertEquals(10, ((NumberType)value).getNumber());
        }

        {
            SingleValueCalcuExpression havingCalcu = (SingleValueCalcuExpression) havingExpression;
            Field field = havingCalcu.getField();
            Calculator calculator = havingCalcu.getCalculator();
            Literal<?> value = havingCalcu.getValue();
            Operator operator = havingCalcu.getOperator();

            assertEquals("click_url", field.getFieldName());
            assertEquals(Calculator.COUNT, calculator);
            assertEquals(Operator.LESS, operator);

            assertTrue(value instanceof NumberType);
            assertEquals(100, ((NumberType)value).getNumber());
        }

        WindowInfoInSQL groupByWindow = windowQueryStatement.getGroupByWindow();
        assertEquals("ts", groupByWindow.getTimeField().getFieldName());
        assertEquals(60, groupByWindow.getSlide());
        assertEquals(60, groupByWindow.getSize());
        assertEquals(WindowInfoInSQL.WindowType.TUMBLE, groupByWindow.getType());
    }

}
