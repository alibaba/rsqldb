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
package com.alibaba.rsqldb.parser.model.statement;

import com.alibaba.rsqldb.common.RSQLConstant;
import com.alibaba.rsqldb.parser.impl.BuildContext;
import com.alibaba.rsqldb.parser.model.Columns;
import com.alibaba.rsqldb.parser.model.FieldType;
import com.alibaba.rsqldb.parser.model.statement.query.FilterQueryStatement;
import com.alibaba.rsqldb.parser.model.statement.query.QueryStatement;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.rocketmq.streams.core.function.FilterAction;
import org.apache.rocketmq.streams.core.rstream.GroupedStream;
import org.apache.rocketmq.streams.core.rstream.RStream;
import org.apache.rocketmq.streams.core.rstream.WindowStream;
import org.apache.rocketmq.streams.core.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * String sql = "INSERT INTO Customers (CustomerName, ContactName, Address, City)\n" +
 *                 "select field_1\n" +
 *                 "     , field_2\n" +
 *                 "     , field_3\n" +
 *                 "     , field_4\n" +
 *                 "from test_source where field_1='1';";
 */
public class InsertQueryStatement extends Statement {
    private static final Logger logger = LoggerFactory.getLogger(InsertQueryStatement.class);
    private QueryStatement queryStatement;
    private Columns columns;

    public InsertQueryStatement(String content, String tableName, QueryStatement queryStatement) {
        super(content, tableName);
        this.queryStatement = queryStatement;
    }

    public InsertQueryStatement(String content, String tableName, QueryStatement queryStatement, Columns columns) {
        super(content, tableName);
        this.queryStatement = queryStatement;
        this.columns = columns;
    }

    public QueryStatement getQueryStatement() {
        return queryStatement;
    }

    public void setQueryStatement(QueryStatement queryStatement) {
        this.queryStatement = queryStatement;
    }

    public Columns getColumns() {
        return columns;
    }

    public void setColumns(Columns columns) {
        this.columns = columns;
    }

    //只需要对queryStatement的结果进行columns过滤即可。
    @Override
    public BuildContext build(BuildContext context) throws Throwable {
        Set<String> fieldName;

        HashMap<String, String> fieldName2NewName = new HashMap<>();
        if (this.columns == null) {
            fieldName = new HashSet<>();
            fieldName.add(RSQLConstant.STAR);
        } else {
            fieldName = this.columns.getFields();
        }
        for (String name : fieldName) {
            fieldName2NewName.put(name, name);
        }

        RStream<JsonNode> stream = context.getrStream();
        WindowStream<String, ? extends JsonNode> windowStream = context.getWindowStream();
        GroupedStream<String, ? extends JsonNode> groupedStream = context.getGroupedStream();
        if (windowStream != null) {
            windowStream = windowStream.map(value -> map(value, fieldName2NewName));
            context.setWindowStream(windowStream);
        } else if (groupedStream != null) {
            groupedStream = groupedStream.map(value -> map(value, fieldName2NewName));
            context.setGroupedStream(groupedStream);
        } else {
            stream = stream.map(value -> map(value, fieldName2NewName));
            context.setrStream(stream);
        }

        return context;
    }
}
