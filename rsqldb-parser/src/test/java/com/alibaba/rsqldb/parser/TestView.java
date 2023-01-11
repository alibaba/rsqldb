/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rsqldb.parser;

import com.alibaba.rsqldb.parser.model.Calculator;
import com.alibaba.rsqldb.parser.model.Field;
import com.alibaba.rsqldb.parser.model.Operator;
import com.alibaba.rsqldb.parser.model.expression.Expression;
import com.alibaba.rsqldb.parser.model.expression.SingleValueExpression;
import com.alibaba.rsqldb.parser.model.statement.CreateViewStatement;
import com.alibaba.rsqldb.parser.model.statement.query.FilterQueryStatement;
import com.alibaba.rsqldb.parser.model.statement.query.QueryStatement;
import com.alibaba.rsqldb.parser.model.statement.query.WindowQueryStatement;
import com.alibaba.rsqldb.parser.model.statement.query.join.JointGroupByHavingStatement;
import com.alibaba.rsqldb.parser.model.statement.query.join.JointStatement;
import com.alibaba.rsqldb.parser.model.statement.query.join.JointWhereGBHavingStatement;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestView extends SerDer {

    @Test
    public void test1() throws Throwable {
        CreateViewStatement createViewStatement = parser(CreateAndInsertSQL.createViewJoin, CreateViewStatement.class);

        QueryStatement queryStatement = createViewStatement.getQueryStatement();
        assertTrue(queryStatement instanceof JointStatement);
        assertEquals("test_view", queryStatement.getTableName());
    }

    //WindowQueryStatement
    @Test
    public void createView1() throws Throwable {
        CreateViewStatement createViewStatement = parser(CreateAndInsertSQL.createViewWindow, CreateViewStatement.class);

        QueryStatement queryStatement = createViewStatement.getQueryStatement();
        assertTrue(queryStatement instanceof WindowQueryStatement);

        WindowQueryStatement windowQueryStatement = (WindowQueryStatement) queryStatement;

        assertEquals("user_clicks", windowQueryStatement.getTableName());
        assertNull(null, windowQueryStatement.getWhereExpression());
        assertNull(null, windowQueryStatement.getHavingExpression());
        assertEquals("ts", windowQueryStatement.getGroupByWindow().getTimeField().getFieldName());
        assertEquals(600, windowQueryStatement.getGroupByWindow().getSize());

        List<Field> groupByFieldList = windowQueryStatement.getGroupByField();

        assertEquals(1, groupByFieldList.size());

        Field groupByField = groupByFieldList.get(0);
        String groupByFieldName = groupByField.getFieldName();
        assertEquals("username", groupByFieldName);

        Map<Field, Calculator> fieldAndCalculator = windowQueryStatement.getSelectFieldAndCalculator();
        assertEquals(3, fieldAndCalculator.size());
        for (Field field : fieldAndCalculator.keySet()) {
            if (field.getFieldName().equals("click_url")) {
                Calculator calculator = fieldAndCalculator.get(field);
                assertEquals(Calculator.COUNT, calculator);
            } else if (field.getFieldName().equals("ts")) {
                Calculator calculator = fieldAndCalculator.get(field);
                assertEquals(Calculator.WINDOW_START, calculator);
            } else if (field.getFieldName().equals("username")) {
                Calculator calculator = fieldAndCalculator.get(field);
                assertNull(calculator);
            }

        }
    }

    //QueryStatement
    @Test
    public void createView2() throws Throwable {
        CreateViewStatement createViewStatement = parser(CreateAndInsertSQL.createViewQuery, CreateViewStatement.class);
        QueryStatement queryStatement = createViewStatement.getQueryStatement();

        assertEquals("user_clicks", queryStatement.getTableName());

        assertEquals(queryStatement.getClass().getName(), QueryStatement.class.getName());

        Map<Field, Calculator> fieldAndCalculator = queryStatement.getSelectFieldAndCalculator();
        assertEquals(3, fieldAndCalculator.size());

        for (Field field : fieldAndCalculator.keySet()) {
            if (field.getFieldName().equals("age")) {
                Calculator calculator = fieldAndCalculator.get(field);
                assertNull(calculator);
                assertEquals("age", field.getAsFieldName());
            } else if (field.getFieldName().equals("username")) {
                Calculator calculator = fieldAndCalculator.get(field);
                assertNull(calculator);
                assertEquals("username", field.getAsFieldName());
            } else if (field.getFieldName().equals("click_url")) {
                Calculator calculator = fieldAndCalculator.get(field);
                assertEquals(Calculator.COUNT, calculator);
                assertEquals("clicks", field.getAsFieldName());
            }
        }
    }

    //FilterQueryStatement
    @Test
    public void createView3() throws Throwable {
        CreateViewStatement createViewStatement = parser(CreateAndInsertSQL.createViewFilterQuery, CreateViewStatement.class);
        QueryStatement queryStatement = createViewStatement.getQueryStatement();

        assertTrue(queryStatement instanceof FilterQueryStatement);

        FilterQueryStatement filterQueryStatement = (FilterQueryStatement) queryStatement;

        assertEquals("user_clicks", filterQueryStatement.getTableName());
        Expression filter = filterQueryStatement.getFilter();

        assertTrue(filter instanceof SingleValueExpression);

        assertEquals(Operator.EQUAL, filter.getOperator());

        SingleValueExpression singleValueExpression = (SingleValueExpression) filter;

        assertEquals(10, singleValueExpression.getValue().result());
        assertEquals("age", singleValueExpression.getField().getFieldName());
    }


    //JointGroupByHavingStatement
    @Test
    public void createView4() throws Throwable {
        CreateViewStatement createViewStatement = parser(CreateAndInsertSQL.createViewJointGroupByHaving, CreateViewStatement.class);

        assertEquals("test_view", createViewStatement.getTableName());

        QueryStatement queryStatement = createViewStatement.getQueryStatement();

        assertTrue(queryStatement instanceof JointGroupByHavingStatement);

        JointGroupByHavingStatement jointGroupByHavingStatement = (JointGroupByHavingStatement) queryStatement;
        assertEquals("access_log", jointGroupByHavingStatement.getTableName());
    }

    //JointWhereGBHavingStatement
    @Test
    public void createView5() throws Throwable {
        CreateViewStatement createViewStatement = parser(CreateAndInsertSQL.createViewJointWhereGBHavingStatement, CreateViewStatement.class);

        assertEquals("viewName", createViewStatement.getTableName());

        QueryStatement queryStatement = createViewStatement.getQueryStatement();
        assertTrue(queryStatement instanceof JointWhereGBHavingStatement);

        JointWhereGBHavingStatement jointWhereGBHavingStatement = (JointWhereGBHavingStatement) queryStatement;

        assertEquals("access_log", jointWhereGBHavingStatement.getTableName());
    }

}
