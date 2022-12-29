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

import com.alibaba.rsqldb.common.exception.SyntaxErrorException;
import com.alibaba.rsqldb.parser.model.statement.Statement;
import org.apache.rocketmq.streams.core.util.Utils;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class TestQuery {
    //----------------------------------where-------------------------------------
    @Test
    public void query1() throws Throwable {
        String sql = "select field_1\n" +
                "     , field_2\n" +
                "     , field_3\n" +
                "     , field_4\n" +
                "from rocketmq_source where field_1='1';";

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
        System.out.println(statements);
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

}
