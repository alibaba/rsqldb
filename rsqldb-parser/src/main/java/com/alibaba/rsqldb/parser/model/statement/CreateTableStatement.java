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
import com.alibaba.rsqldb.parser.model.baseType.Literal;
import com.alibaba.rsqldb.parser.util.Pair;
import org.antlr.v4.runtime.ParserRuleContext;

import java.util.ArrayList;
import java.util.List;

public class CreateTableStatement extends Node {
    private String tableName;
    private Columns columns;
    private List<Pair<String, Literal<?>>> properties;

    public CreateTableStatement(ParserRuleContext context, String tableName, Columns columns, List<Pair<String, Literal<?>>> properties) {
        super(context);
        this.tableName = tableName;
        this.columns = columns;
        this.properties = properties;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Columns getColumns() {
        return columns;
    }

    public void setColumns(Columns columns) {
        this.columns = columns;
    }

    public List<Pair<String, Literal<?>>> getProperties() {
        return properties;
    }

    public void setProperties(List<Pair<String, Literal<?>>> properties) {
        this.properties = properties;
    }

    @Override
    public String toString() {
        return "Table{" +
                "tableName='" + tableName + '\'' +
                ", columns=" + columns +
                ", properties=" + properties +
                '}';
    }
}
