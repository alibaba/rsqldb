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
import com.alibaba.rsqldb.parser.model.ColumnValue;
import com.alibaba.rsqldb.parser.model.Columns;
import com.alibaba.rsqldb.parser.model.statement.InsertQueryStatement;
import com.alibaba.rsqldb.parser.model.statement.InsertValueStatement;
import com.alibaba.rsqldb.parser.model.statement.Statement;
import com.alibaba.rsqldb.parser.model.statement.query.FilterQueryStatement;
import com.alibaba.rsqldb.parser.model.statement.query.QueryStatement;
import com.alibaba.rsqldb.parser.serialization.Deserializer;
import com.alibaba.rsqldb.parser.serialization.SerializeTypeContainer;
import com.alibaba.rsqldb.parser.serialization.Serializer;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestInsert {

    @Test
    public void insert1() throws Throwable {
        InsertValueStatement insertQueryStatement = insertCommon(CreateAndInsertSQL.insertValue, InsertValueStatement.class);

        String statementTableName = insertQueryStatement.getTableName();
        assertEquals("purchaser_dim", statementTableName);

        List<ColumnValue> columns = insertQueryStatement.getColumns();
        assertEquals(4, columns.size());
    }

    private <T> T insertCommon(String sql, Class<T> clazz) throws Throwable {
        DefaultParser parser = new DefaultParser();
        List<Statement> statements = parser.parseStatement(sql);
        assertEquals(1, statements.size());

        Statement statement = statements.get(0);

        Serializer serializer = SerializeTypeContainer.getSerializer(SerializeType.JSON);
        byte[] bytes = serializer.serialize(statement);


        Deserializer deserializer = SerializeTypeContainer.getDeserializer(SerializeType.JSON);

        return deserializer.deserialize(bytes, clazz);
    }


    @Test
    public void insert2() throws Throwable {
        InsertValueStatement insertValueStatement = insertCommon(CreateAndInsertSQL.insertValueField, InsertValueStatement.class);
        String statementTableName = insertValueStatement.getTableName();
        assertEquals("Customers", statementTableName);

        List<ColumnValue> columns = insertValueStatement.getColumns();
        assertEquals(6, columns.size());
    }

    @Test
    public void insert3() throws Throwable {
        InsertQueryStatement insertQueryStatement = insertCommon(CreateAndInsertSQL.insertSelect, InsertQueryStatement.class);

        assertEquals("test_sink", insertQueryStatement.getTableName());

        Columns columns = insertQueryStatement.getColumns();
        assertNull(columns);

        QueryStatement queryStatement = insertQueryStatement.getQueryStatement();
        assertTrue(queryStatement instanceof FilterQueryStatement);
    }

    @Test
    public void insert4() throws Throwable {
        InsertQueryStatement insertQueryStatement = insertCommon(CreateAndInsertSQL.insertSelectField, InsertQueryStatement.class);

        assertEquals("Customers", insertQueryStatement.getTableName());

        Columns columns = insertQueryStatement.getColumns();
        assertEquals(4, columns.getHolder().size());

        QueryStatement queryStatement = insertQueryStatement.getQueryStatement();

        assertTrue(queryStatement instanceof FilterQueryStatement);
    }
}
