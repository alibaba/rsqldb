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

import com.alibaba.rsqldb.parser.model.Columns;
import com.alibaba.rsqldb.parser.model.Node;
import com.alibaba.rsqldb.parser.model.statement.query.QueryStatement;

import java.util.ArrayList;
import java.util.List;

public class InsertQueryStatement extends Node {
    private String sinkTableName;
    private QueryStatement queryStatement;
    private Columns columns;

    public InsertQueryStatement(String sinkTableName, QueryStatement queryStatement) {
        this.sinkTableName = sinkTableName;
        this.queryStatement = queryStatement;
    }

    public InsertQueryStatement(String sinkTableName, QueryStatement queryStatement, Columns columns) {
        this.sinkTableName = sinkTableName;
        this.queryStatement = queryStatement;
        this.columns = columns;
    }

    public String getSinkTableName() {
        return sinkTableName;
    }

    public void setSinkTableName(String sinkTableName) {
        this.sinkTableName = sinkTableName;
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
}
