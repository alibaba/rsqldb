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
import com.alibaba.rsqldb.parser.model.statement.CreateTableStatement;
import com.alibaba.rsqldb.parser.model.statement.Statement;
import com.alibaba.rsqldb.parser.serialization.Deserializer;
import com.alibaba.rsqldb.parser.serialization.SerializeTypeContainer;
import com.alibaba.rsqldb.parser.serialization.Serializer;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestCreate {

    @Test
    public void createTable1() throws Throwable {
        //在properties中指定timeField字段，指定允许延迟时间。
        String sql = CreateAndInsertSQL.createTable;

        DefaultParser parser = new DefaultParser();
        List<Statement> statements = parser.parseStatement(sql);

        assertEquals(1, statements.size());

        Statement statement = statements.get(0);

        Serializer serializer = SerializeTypeContainer.getSerializer(SerializeType.JSON);
        byte[] bytes = serializer.serialize(statement);


        Deserializer deserializer = SerializeTypeContainer.getDeserializer(SerializeType.JSON);
        CreateTableStatement target = deserializer.deserialize(bytes, CreateTableStatement.class);

        assertNotNull(target);
        assertEquals("odeum", target.getTableName());
        assertEquals(3, target.getColumns().getHolder().size());
        assertEquals("rsqldb-odeum", target.getTopicName());
        assertEquals(SerializeType.JSON, target.getSerializeType());
    }



}
