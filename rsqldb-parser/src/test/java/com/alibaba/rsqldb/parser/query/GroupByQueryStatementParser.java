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

import com.alibaba.rsqldb.common.exception.SyntaxErrorException;
import com.alibaba.rsqldb.parser.SerDer;
import com.alibaba.rsqldb.parser.model.Calculator;
import com.alibaba.rsqldb.parser.model.Field;
import com.alibaba.rsqldb.parser.model.Operator;
import com.alibaba.rsqldb.parser.model.baseType.Literal;
import com.alibaba.rsqldb.parser.model.baseType.NumberType;
import com.alibaba.rsqldb.parser.model.expression.AndExpression;
import com.alibaba.rsqldb.parser.model.expression.Expression;
import com.alibaba.rsqldb.parser.model.expression.SingleValueCalcuExpression;
import com.alibaba.rsqldb.parser.model.expression.SingleValueExpression;
import com.alibaba.rsqldb.parser.model.statement.query.GroupByQueryStatement;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class GroupByQueryStatementParser extends SerDer {

    @Test
    public void test0() throws Throwable {
        String sql = "SELECT `position`, avg(num) AS nums\n" +
                "FROM sourceTable\n" +
                "GROUP BY position;";

        GroupByQueryStatement groupStatement = parser(sql, GroupByQueryStatement.class);
        assertEquals("sourceTable", groupStatement.getTableName());

        List<Field> groupByField = groupStatement.getGroupByField();
        Map<Field, Calculator> selectFieldAndCalculator = groupStatement.getSelectFieldAndCalculator();
        Expression whereExpression = groupStatement.getWhereExpression();
        Expression havingExpression = groupStatement.getHavingExpression();

        assertEquals(1, groupByField.size());
        assertEquals("position", groupByField.get(0).getFieldName());
        assertEquals(2, selectFieldAndCalculator.size());
        assertNull(whereExpression);
        assertNull(havingExpression);

        for (Field field : selectFieldAndCalculator.keySet()) {
            if (field.getFieldName().equals("position")) {
                String tableName = field.getTableName();
                String asFieldName = field.getAsFieldName();
                assertNull(tableName);
                assertNull(asFieldName);
            } else if (field.getFieldName().equals("num")) {
                String tableName = field.getTableName();
                String asFieldName = field.getAsFieldName();

                assertNull(tableName);
                assertEquals("nums", asFieldName);

                Calculator calculator = selectFieldAndCalculator.get(field);
                assertSame(Calculator.AVG, calculator);
            } else {
                throw new IllegalStateException();
            }
        }
    }

    @Test
    public void test1() throws Throwable {
        String sql = "SELECT `position`, avg(num) AS nums\n" +
                "FROM sourceTable\n" +
                "WHERE num <= 100 " +
                "GROUP BY position " +
                "HAVING avg(num) < 50";

        GroupByQueryStatement groupStatement = parser(sql, GroupByQueryStatement.class);
        assertEquals("sourceTable", groupStatement.getTableName());


        Expression whereExpression = groupStatement.getWhereExpression();
        Expression havingExpression = groupStatement.getHavingExpression();

        assertTrue(whereExpression instanceof SingleValueExpression);
        assertTrue(havingExpression instanceof SingleValueCalcuExpression);

        {
            SingleValueExpression singleValueExpression = (SingleValueExpression) whereExpression;
            Literal<?> value = singleValueExpression.getValue();
            Operator operator = singleValueExpression.getOperator();
            Field field = singleValueExpression.getField();

            assertEquals("num", field.getFieldName());
            assertNull(field.getTableName());
            assertNull(field.getAsFieldName());
            assertSame(Operator.LESS_EQUAL, operator);

            assertTrue(value instanceof NumberType);
            assertEquals(100, ((NumberType) value).getNumber());
        }

        {
            SingleValueCalcuExpression havingCalcu = (SingleValueCalcuExpression) havingExpression;
            Field field = havingCalcu.getField();
            Calculator calculator = havingCalcu.getCalculator();
            Literal<?> value = havingCalcu.getValue();
            Operator operator = havingCalcu.getOperator();

            assertEquals("num", field.getFieldName());
            assertEquals(Calculator.AVG, calculator);
            assertEquals(Operator.LESS, operator);

            assertTrue(value instanceof NumberType);
            assertEquals(50, ((NumberType) value).getNumber());
        }

    }

    @Test
    public void test2() throws Throwable {
        String sql = "SELECT `position`, avg(num) AS nums, sum(num) AS countNum\n" +
                "FROM sourceTable\n" +
                "GROUP BY position " +
                "HAVING avg(num) < 50 and sum(num) > 500";

        GroupByQueryStatement groupStatement = parser(sql, GroupByQueryStatement.class);
        Expression whereExpression = groupStatement.getWhereExpression();
        Expression havingExpression = groupStatement.getHavingExpression();


        assertNull(whereExpression);

        assertTrue(havingExpression instanceof AndExpression);

        AndExpression andExpression = (AndExpression) havingExpression;
        Expression leftExpression = andExpression.getLeftExpression();
        Expression rightExpression = andExpression.getRightExpression();

        assertTrue(leftExpression instanceof SingleValueExpression);
        assertTrue(rightExpression instanceof SingleValueExpression);

        {
            SingleValueCalcuExpression valueExpression = (SingleValueCalcuExpression) leftExpression;
            assertEquals("num", valueExpression.getField().getFieldName());
            assertEquals(50, ((NumberType) valueExpression.getValue()).getNumber());
            assertEquals(Operator.LESS, valueExpression.getOperator());
            assertEquals(Calculator.AVG, valueExpression.getCalculator());
        }

        {
            SingleValueCalcuExpression valueExpression = (SingleValueCalcuExpression) rightExpression;
            assertEquals("num", valueExpression.getField().getFieldName());
            assertEquals(500, ((NumberType) valueExpression.getValue()).getNumber());
            assertEquals(Operator.GREATER, valueExpression.getOperator());
            assertEquals(Calculator.SUM, valueExpression.getCalculator());
        }
    }

    @Test
    public void test3() throws Throwable {
        String sql = "SELECT `position`, avg(num_1) AS nums, sum(num) AS countNum\n" +
                "FROM sourceTable\n" +
                "GROUP BY position " +
                "HAVING avg(num) < 50 and sum(num) > 500";

        Throwable error = null;
        try {
            GroupByQueryStatement groupStatement = parser(sql, GroupByQueryStatement.class);
        } catch (Throwable t) {
            error = t;
        }

        assertNotNull(error);
        assertTrue(error instanceof SyntaxErrorException);
        assertTrue(error.getMessage().startsWith("field in having but not in select."));
    }

}
