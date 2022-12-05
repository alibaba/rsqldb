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

import com.alibaba.rsqldb.parser.model.ColumnValue;
import com.alibaba.rsqldb.parser.model.Node;
import org.antlr.v4.runtime.ParserRuleContext;

import java.util.ArrayList;
import java.util.List;

public class InsertValueStatement extends Node {
    private String name;
    private List<ColumnValue> columns = new ArrayList<>();

    public InsertValueStatement(ParserRuleContext context, String name, List<ColumnValue> columns) {
        super(context);
        this.name = name;
        this.columns = columns;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<ColumnValue> getColumns() {
        return columns;
    }

    public void setColumns(List<ColumnValue> columns) {
        this.columns = columns;
    }

    @Override
    public String toString() {
        return "Insert{" +
                "name='" + name + '\'' +
                ", columns=" + columns +
                '}';
    }
}