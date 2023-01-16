 /*
  * Copyright 1999-2018 Alibaba Group Holding Ltd.
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
import com.alibaba.rsqldb.parser.model.statement.query.join.JointGroupByHavingStatement;
import com.alibaba.rsqldb.parser.model.statement.query.join.JointWhereGBHavingStatement;
import com.alibaba.rsqldb.parser.model.statement.query.join.JointWhereStatement;
import com.alibaba.rsqldb.parser.model.statement.query.phrase.JoinCondition;
import com.alibaba.rsqldb.parser.model.statement.query.phrase.JoinType;
import org.apache.rocketmq.streams.core.util.Pair;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class JoinParser extends SerDer {
    @Test
    public void test0() throws Throwable {
        String sql = "SELECT Websites.name as count, Websites.url as url, SUM(access_log.count) AS nums FROM access_log " +
                " INNER JOIN Websites ON access_log.site_id=Websites.id\n" +
                " GROUP BY Websites.name\n" +
                " HAVING SUM(access_log.`count`) > 200;";//todo having 和select中不一样； select中表名的作用。

        JointGroupByHavingStatement havingStatement = super.parser(sql, JointGroupByHavingStatement.class);

        assertNotNull(havingStatement);
        assertEquals("access_log", havingStatement.getTableName());
        assertEquals("Websites", havingStatement.getJoinTableName());

        Expression havingExpression = havingStatement.getHavingExpression();
        List<Field> groupByFieldList = havingStatement.getGroupByField();
        JoinType joinType = havingStatement.getJoinType();
        Map<Field, Calculator> fieldAndCalculator = havingStatement.getSelectFieldAndCalculator();
        JoinCondition joinCondition = havingStatement.getJoinCondition();

        assertTrue(havingExpression instanceof SingleValueCalcuExpression);
        SingleValueCalcuExpression valueCalcuExpression = (SingleValueCalcuExpression) havingExpression;
        Operator operator = valueCalcuExpression.getOperator();
        Calculator calculator = valueCalcuExpression.getCalculator();
        Literal<?> value = valueCalcuExpression.getValue();
        assertEquals(operator, Operator.GREATER);
        assertEquals(calculator, Calculator.SUM);
        assertTrue(value instanceof NumberType);
        assertEquals(((NumberType) value).result().intValue(), 200);


        assertEquals(groupByFieldList.size(), 1);
        Field groupByField = groupByFieldList.get(0);
        assertEquals(groupByField.getTableName(), "Websites");
        assertEquals(groupByField.getFieldName(), "name");

        assertEquals(joinType, JoinType.INNER_JOIN);

        List<Pair<Field, Field>> holder = joinCondition.getHolder();
        assertEquals(holder.size(), 1);
        Pair<Field, Field> pair = holder.get(0);
        Field firstField = pair.getKey();
        Field secondField = pair.getValue();
        assertEquals(firstField.getTableName(), "access_log");
        assertEquals(firstField.getFieldName(), "site_id");
        assertEquals(secondField.getTableName(), "Websites");
        assertEquals(secondField.getFieldName(), "id");

        assertEquals(3, fieldAndCalculator.size());

        for (Field field : fieldAndCalculator.keySet()) {
            if (field.getAsFieldName().equals("count")) {
                assertEquals(field.getTableName(), "Websites");
                assertEquals(field.getFieldName(), "name");
            } else if (field.getAsFieldName().equals("url")) {
                assertEquals(field.getTableName(), "Websites");
                assertEquals(field.getFieldName(), "url");
            } else if (field.getAsFieldName().equals("nums")) {
                assertEquals(field.getTableName(), "access_log");
                assertEquals(field.getFieldName(), "count");
                Calculator calculator1 = fieldAndCalculator.get(field);
                assertEquals(calculator1, Calculator.SUM);
            } else {
                throw new IllegalStateException("unexpected as name." + field.getAsFieldName());
            }
        }
    }

    @Test
    public void test1() throws Throwable {
        String sql = "SELECT Websites.name as `count`, Websites.url as url, SUM(access_log.count) AS nums " +
                " FROM access_log " +
                " WHERE access_log.`count` > 100" +
                " INNER JOIN Websites ON access_log.site_id=Websites.id\n" +
                " GROUP BY Websites.name\n" +
                " HAVING SUM(access_log.`count`) <= 2000;";//todo having 和select中不一样； select中表名的作用。

        JointWhereGBHavingStatement statement = super.parser(sql, JointWhereGBHavingStatement.class);

        assertEquals("access_log", statement.getTableName());
        assertEquals(JoinType.INNER_JOIN, statement.getJoinType());
        assertEquals("Websites", statement.getJoinTableName());
        assertNull(statement.getAsJoinTableName());

        List<Pair<Field, Field>> holder = statement.getJoinCondition().getHolder();
        assertEquals(1, holder.size());
        assertEquals("access_log", holder.get(0).getKey().getTableName());
        assertEquals("site_id", holder.get(0).getKey().getFieldName());
        assertEquals("Websites", holder.get(0).getValue().getTableName());
        assertEquals("id", holder.get(0).getValue().getFieldName());

        List<Field> groupByField = statement.getGroupByField();
        assertEquals(1, groupByField.size());
        assertEquals("Websites", groupByField.get(0).getTableName());
        assertEquals("name", groupByField.get(0).getFieldName());

        Expression beforeWhere = statement.getBeforeJoinWhereExpression();
        assertTrue(beforeWhere instanceof SingleValueExpression);
        SingleValueExpression valueExpression = (SingleValueExpression) beforeWhere;
        assertEquals("access_log", valueExpression.getField().getTableName());
        assertEquals("count", valueExpression.getField().getFieldName());
        assertEquals(Operator.GREATER, valueExpression.getOperator());
        assertEquals(100, ((NumberType) valueExpression.getValue()).getNumber());

        Expression havingExpression = statement.getHavingExpression();
        assertTrue(havingExpression instanceof SingleValueCalcuExpression);
        SingleValueCalcuExpression calcuExpression = (SingleValueCalcuExpression) havingExpression;
        assertEquals("access_log", calcuExpression.getField().getTableName());
        assertEquals("count", calcuExpression.getField().getFieldName());
        assertEquals(Operator.LESS_EQUAL, calcuExpression.getOperator());
        assertEquals(2000, ((NumberType) calcuExpression.getValue()).getNumber());
        assertSame(Calculator.SUM, calcuExpression.getCalculator());
    }

    @Test
    public void test2() throws Throwable {
        String sql = "SELECT Websites.name as `count`, Websites.url as url, SUM(access_log.count) AS nums " +
                " FROM access_log " +
                " WHERE access_log.`count` > 100" +
                " INNER JOIN Websites ON access_log.site_id=Websites.id and access_log.url=Websites.url";

        JointWhereStatement statement = super.parser(sql, JointWhereStatement.class);

        JoinCondition joinCondition = statement.getJoinCondition();
        List<Pair<Field, Field>> holder = joinCondition.getHolder();

        assertEquals(2, holder.size());

        {
            Pair<Field, Field> fieldPair = holder.get(0);
            assertEquals("access_log", fieldPair.getKey().getTableName());
            assertEquals("site_id", fieldPair.getKey().getFieldName());
            assertEquals("Websites", fieldPair.getValue().getTableName());
            assertEquals("id", fieldPair.getValue().getFieldName());
        }

        {
            Pair<Field, Field> fieldPair = holder.get(1);
            assertEquals("access_log", fieldPair.getKey().getTableName());
            assertEquals("url", fieldPair.getKey().getFieldName());
            assertEquals("Websites", fieldPair.getValue().getTableName());
            assertEquals("url", fieldPair.getValue().getFieldName());
        }

    }
}
