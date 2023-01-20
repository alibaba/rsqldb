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
package com.alibaba.rsqldb.parser.model.statement;

import com.alibaba.rsqldb.common.RSQLConstant;
import com.alibaba.rsqldb.parser.impl.BuildContext;
import com.alibaba.rsqldb.parser.model.Columns;
import com.alibaba.rsqldb.parser.model.statement.query.QueryStatement;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.rocketmq.streams.core.rstream.GroupedStream;
import org.apache.rocketmq.streams.core.rstream.RStream;
import org.apache.rocketmq.streams.core.rstream.WindowStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * String sql = "INSERT INTO Customers (CustomerName, ContactName, Address, City)\n" +
 * "select field_1\n" +
 * "     , field_2\n" +
 * "     , field_3\n" +
 * "     , field_4\n" +
 * "from test_source where field_1='1';";
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class InsertQueryStatement extends Statement {
    private static final Logger logger = LoggerFactory.getLogger(InsertQueryStatement.class);
    private QueryStatement queryStatement;
    private Columns columns;


    public InsertQueryStatement(String content, String tableName, QueryStatement queryStatement) {
        super(content, tableName);
        this.queryStatement = queryStatement;
    }

    @JsonCreator
    public InsertQueryStatement(@JsonProperty("content") String content, @JsonProperty("tableName") String tableName,
                                @JsonProperty("queryStatement") QueryStatement queryStatement, @JsonProperty("columns") Columns columns) {
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

        //the right RStream will in StreamResult field, because build queryStatement will happen before this.
        RStream<? extends JsonNode> stream = context.getrStreamResult();

        WindowStream<String, ? extends JsonNode> windowStream = context.getWindowStreamResult();
        GroupedStream<String, ? extends JsonNode> groupedStream = context.getGroupedStreamResult();
        if (windowStream != null) {
            windowStream = windowStream.map(value -> map(value, fieldName2NewName));
            context.setWindowStreamResult(windowStream);
        } else if (groupedStream != null) {
            groupedStream = groupedStream.map(value -> map(value, fieldName2NewName));
            context.setGroupedStreamResult(groupedStream);
        } else {
            stream = stream.map(value -> map(value, fieldName2NewName));
            context.setrStreamResult(stream);
        }

        return context;
    }
}
