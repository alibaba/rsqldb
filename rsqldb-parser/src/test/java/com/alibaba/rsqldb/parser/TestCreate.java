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


import com.alibaba.rsqldb.common.SerializeType;
import com.alibaba.rsqldb.parser.model.Node;
import com.alibaba.rsqldb.parser.model.statement.CreateTableStatement;
import com.alibaba.rsqldb.parser.model.statement.Statement;
import com.alibaba.rsqldb.parser.serialization.Deserializer;
import com.alibaba.rsqldb.parser.serialization.SerializeTypeContainer;
import com.alibaba.rsqldb.parser.serialization.Serializer;
import org.junit.Test;

import java.util.List;

public class TestCreate {

    @Test
    public void create1() throws Throwable {
        //在properties中指定timeField字段，指定允许延迟时间。
        String sql = "create table odeum(`id` INT,`name` VARCHAR, `gmt_modified` TIMESTAMP) WITH (type = null, topic = 'rsqldb-odeum', data_format='JSON');";

        DefaultParser parser = new DefaultParser();
        List<Statement> statements = parser.parseStatement(sql);

        for (Statement statement : statements) {
            System.out.println(statement);
            Serializer serializer = SerializeTypeContainer.getSerializer(SerializeType.JSON);
            byte[] bytes = serializer.serialize(statement);


            Deserializer deserializer = SerializeTypeContainer.getDeserializer(SerializeType.JSON);
            Node deserialize = deserializer.deserialize(bytes, CreateTableStatement.class);

            System.out.println(deserialize);
        }
    }


    @Test
    public void create2() throws Throwable {
        String sql = "CREATE VIEW test_view AS\n" +
                "SELECT\n" +
                "    TUMBLE_START(ts, INTERVAL '10' MINUTE)     as window_start,\n" +
                "    username                                as username,\n" +
                "    count(click_url)                        as clicks\n" +
                "FROM user_clicks\n" +
                "GROUP BY TUMBLE(ts, INTERVAL '10' MINUTE), username;";

        DefaultParser parser = new DefaultParser();
        parser.parseStatement(sql);
    }
}
