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
package com.alibaba.rsqldb.parser;

import com.alibaba.rsqldb.common.SerializeType;
import com.alibaba.rsqldb.common.exception.SyntaxErrorException;
import com.alibaba.rsqldb.parser.model.Calculator;
import com.alibaba.rsqldb.parser.model.Field;
import com.alibaba.rsqldb.parser.model.Node;
import com.alibaba.rsqldb.parser.model.Operator;
import com.alibaba.rsqldb.parser.model.baseType.Literal;
import com.alibaba.rsqldb.parser.model.baseType.NumberType;
import com.alibaba.rsqldb.parser.model.expression.Expression;
import com.alibaba.rsqldb.parser.model.expression.SingleValueCalcuExpression;
import com.alibaba.rsqldb.parser.model.statement.InsertQueryStatement;
import com.alibaba.rsqldb.parser.model.statement.Statement;
import com.alibaba.rsqldb.parser.model.statement.query.join.JointGroupByHavingStatement;
import com.alibaba.rsqldb.parser.model.statement.query.phrase.JoinCondition;
import com.alibaba.rsqldb.parser.model.statement.query.phrase.JoinType;
import com.alibaba.rsqldb.parser.serialization.Deserializer;
import com.alibaba.rsqldb.parser.serialization.SerializeTypeContainer;
import com.alibaba.rsqldb.parser.serialization.Serializer;
import org.apache.rocketmq.streams.core.util.Pair;
import org.apache.rocketmq.streams.core.util.Utils;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestQuery {
    //----------------------------------where-------------------------------------
    @Test
    public void query1() throws Throwable {
        String sql = "select field_1\n" +
                "     , field_2\n" +
                "     , field_3\n" +
                "     , field_4\n" +
                "from rocketmq_source where field_1=23;";

        DefaultParser parser = new DefaultParser();
        List<Statement> statements = parser.parseStatement(sql);

        System.out.println(statements);
    }

    @Test
    public void query2() throws Throwable {
        String sql = "select field_1\n" +
                "     , field_2\n" +
                "     , field_3\n" +
                "     , field_4\n" +
                "from rocketmq_source where field_1 is null;";

        DefaultParser parser = new DefaultParser();
        parser.parseStatement(sql);
    }

    @Test
    public void query3() throws Throwable {
        String sql = "select field_1\n" +
                "     , field_2\n" +
                "     , field_3\n" +
                "     , field_4\n" +
                "from rocketmq_source where field_1 between 1 and 10;";

        DefaultParser parser = new DefaultParser();
        parser.parseStatement(sql);
    }

    @Test
    public void query4() throws Throwable {
        String sql = "select field_1\n" +
                "     , field_2\n" +
                "     , field_3\n" +
                "     , field_4\n" +
                "from rocketmq_source where field_1 in('qw', '1q2', 'w2q', 122, \"dss\");";

        DefaultParser parser = new DefaultParser();
        parser.parseStatement(sql);
    }

    //-----------------------------------------------------select item-----------------------------------------------------------------------
    @Test
    public void query10() throws Throwable {
        String sql = "select * from rocketmq_source where field_1='1';";

        DefaultParser parser = new DefaultParser();
        parser.parseStatement(sql);
    }

    @Test
    public void query11() throws Throwable {
        String sql = "select oldName as newName from rocketmq_source where field_1='1';";

        DefaultParser parser = new DefaultParser();
        parser.parseStatement(sql);
    }

    @Test
    public void query12() throws Throwable {
        String sql = "select `tableName`.`fei_e3` as newName from rocketmq_source where field_1='1';";

        DefaultParser parser = new DefaultParser();
        parser.parseStatement(sql);
    }

    @Test
    public void query13() throws Throwable {
        String sql = "select count(`fieldName`) as newName from rocketmq_source where field_1='1';";

        DefaultParser parser = new DefaultParser();
        parser.parseStatement(sql);
    }

    @Test
    public void query14() throws Throwable {
        String sql = "select count(*) as newName from rocketmq_source where field_1='1';";

        DefaultParser parser = new DefaultParser();
        parser.parseStatement(sql);
    }

    @Test
    public void query15() throws Throwable {
        String sql = "select count(tableName.fieldName) as newName from rocketmq_source where field_1='1';";

        DefaultParser parser = new DefaultParser();
        parser.parseStatement(sql);
    }

    //todo 这种格式是错误的，window，需要groupBy字段
    @Test
    public void query16() throws Throwable {
        String sql = "SELECT\n" +
                "    TUMBLE_START(ts, INTERVAL '1' MINUTE)   as window_start,\n" +
                "    TUMBLE_END(ts, INTERVAL '1' MINUTE)     as window_end,\n" +
                "    username                                as username,\n" +
                "    count(click_url)                        as clicks\n" +
                "FROM user_clicks;";

        DefaultParser parser = new DefaultParser();
        SyntaxErrorException errorException = null;
        try {
            parser.parseStatement(sql);
        } catch (SyntaxErrorException e) {
            errorException = e;
        }
        assert errorException != null;
    }

    @Test
    public void query17() throws Throwable {
        String sql = "SELECT\n" +
                "    SESSION_START(ts, INTERVAL '1' SECOND)   as window_start,\n" +
                "    SESSION_END(ts, INTERVAL '1' SECOND)     as window_end,\n" +
                "    username                                as username,\n" +
                "    count(click_url)                        as clicks\n" +
                "FROM user_clicks";

        DefaultParser parser = new DefaultParser();
        SyntaxErrorException errorException = null;
        try {
            parser.parseStatement(sql);
        } catch (SyntaxErrorException e) {
            errorException = e;
        }
        assert errorException != null;
    }

    @Test
    public void query18() throws Throwable {
        String sql = "SELECT\n" +
                "    HOP_START (ts, INTERVAL '30' SECOND, INTERVAL '1' MINUTE)              as window_start,\n" +
                "    HOP_END (ts, INTERVAL '30' SECOND, INTERVAL '1' MINUTE)                as window_end,\n" +
                "    username                                as username,\n" +
                "    count(click_url)                        as clicks\n" +
                "FROM user_clicks;";

        DefaultParser parser = new DefaultParser();
        SyntaxErrorException errorException = null;
        try {
            parser.parseStatement(sql);
        } catch (SyntaxErrorException e) {
            errorException = e;
        }
        assert errorException != null;
    }

    //-----------------------------------------------------select item-----------------------------------------------------------------------

    @Test
    public void query20() throws Throwable {
        String sql = "SELECT `position`, avg(num) AS nums\n" +
                "FROM source_function_0\n" +
                "GROUP BY position;";

        DefaultParser parser = new DefaultParser();
        List<Statement> statements = parser.parseStatement(sql);
        System.out.println(statements);
    }

    @Test
    public void query21() throws Throwable {
        String sql = "SELECT\n" +
                "    TUMBLE_START(ts, INTERVAL '1' MINUTE)       AS  window_start,\n" +
                "    TUMBLE_END(ts, INTERVAL '1' MINUTE)         AS  window_end,\n" +
                "    username                                    AS  username,\n" +
                "    COUNT(click_url)                            AS  clicks\n" +
                "FROM window_test\n" +
                "GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE), username;";

        DefaultParser parser = new DefaultParser();
        parser.parseStatement(sql);
    }

    @Test
    public void query22() throws Throwable {
        String sql = "INSERT INTO session_output\n" +
                "SELECT\n" +
                "    SESSION_START(ts, INTERVAL '30' SECOND)     as window_start,\n" +
                "    SESSION_END(ts, INTERVAL '30' SECOND)       as window_end,\n" +
                "    username                                    as username,\n" +
                "    COUNT(click_url)                            as clicks\n" +
                "FROM window_test\n" +
                "GROUP BY SESSION(ts, INTERVAL '30' SECOND), username;";

        DefaultParser parser = new DefaultParser();
        List<Statement> statements = parser.parseStatement(sql);

        for (Statement statement : statements) {
            System.out.println(statement);
            Serializer serializer = SerializeTypeContainer.getSerializer(SerializeType.JSON);
            byte[] bytes = serializer.serialize(statement);


            Deserializer deserializer = SerializeTypeContainer.getDeserializer(SerializeType.JSON);
            Node deserialize = deserializer.deserialize(bytes, InsertQueryStatement.class);

            System.out.println(deserialize);
        }
    }

    @Test
    public void query23() throws Throwable {
        String sql = "SELECT\n" +
                "    HOP_START (ts, INTERVAL '30' SECOND, INTERVAL '1' MINUTE)   as window_start,\n" +
                "    HOP_END (ts, INTERVAL '30' SECOND, INTERVAL '1' MINUTE)     as window_end,\n" +
                "    username                                                    as username,\n" +
                "    COUNT(click_url)                                            as clicks\n" +
                "FROM user_clicks\n" +
                "GROUP BY HOP (ts, INTERVAL '30' SECOND, INTERVAL '1' MINUTE), username;";

        DefaultParser parser = new DefaultParser();
        List<Statement> statements = parser.parseStatement(sql);
        System.out.println(statements);
    }

    @Test
    public void query24() throws Throwable {
        String sql = "SELECT `position`, avg(num) AS nums\n" +
                "FROM source_function_0\n" +
                "WHERE position= 'shenzhen'\n" +
                "GROUP BY position\n" +
                "HAVING avg(num) > 10;";

        DefaultParser parser = new DefaultParser();
        List<Statement> statements = parser.parseStatement(sql);
        System.out.println(statements);
    }


    //-----------------------------------------------------join--------------------------------------------------------------------------
    @Test
    public void query30() throws Throwable {
        String sql = "SELECT t.id         AS ticket_id,\n" +
                "       t.`position` AS `position`,\n" +
                "       p.name       AS perform_name,\n" +
                "       p.odeum_id   AS odeum_id\n" +
                "FROM ticket AS t\n"
                + "where t.id > 100"
                + "         LEFT JOIN perform AS p ON t.perform_id = p.id "
                + "where p.name = 'nize';";

        DefaultParser parser = new DefaultParser();
        List<Statement> statements = parser.parseStatement(sql);

        System.out.println(statements);

        byte[] bytes = sql.getBytes(StandardCharsets.UTF_8);
        String str = Utils.toHexString(bytes);
        System.out.println(str);
    }

    @Test
    public void query31() throws Throwable {
        String sql = "SELECT Websites.name as `count`, Websites.url as url, SUM(access_log.count) AS nums FROM access_log " +
                " INNER JOIN Websites ON access_log.site_id=Websites.id\n" +
                " GROUP BY Websites.name\n" +
                " HAVING SUM(access_log.`count`) > 200;";//todo having 和select中不一样； select中表名的作用。

        DefaultParser parser = new DefaultParser();
        List<Statement> statements = parser.parseStatement(sql);

        assertEquals(1, statements.size());
        Statement statement = statements.get(0);

        JointGroupByHavingStatement havingStatement = serde(statement, JointGroupByHavingStatement.class);

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

    private <T> T serde(Statement statement, Class<T> clazz) throws Throwable {
        Serializer serializer = SerializeTypeContainer.getSerializer(SerializeType.JSON);
        byte[] bytes = serializer.serialize(statement);


        Deserializer deserializer = SerializeTypeContainer.getDeserializer(SerializeType.JSON);
        return deserializer.deserialize(bytes, clazz);
    }

}
