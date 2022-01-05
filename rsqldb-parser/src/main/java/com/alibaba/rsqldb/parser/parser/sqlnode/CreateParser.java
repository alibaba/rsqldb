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
package com.alibaba.rsqldb.parser.parser.sqlnode;

import com.alibaba.rsqldb.parser.parser.builder.CreateSQLBuilder;
import com.alibaba.rsqldb.parser.parser.result.BuilderParseResult;
import com.alibaba.rsqldb.parser.parser.result.IParseResult;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;

import java.util.List;

/**
 * Create Table Parser
 */
public class CreateParser extends AbstractSqlNodeParser<SqlCreateTable, CreateSQLBuilder> {

    private static final Log LOG = LogFactory.getLog(CreateParser.class);

    @Override
    public IParseResult parse(CreateSQLBuilder tableDescriptor, SqlCreateTable sqlCreateTable) {
        tableDescriptor.setSqlNode(sqlCreateTable);
        tableDescriptor.setSqlType(sqlCreateTable.getTableType());
        tableDescriptor.setTableName(sqlCreateTable.getTableName().toString());
        tableDescriptor.createColumn(sqlCreateTable.getColumnList());
        tableDescriptor.addCreatedTable(tableDescriptor.getTableName());
        SqlNodeList sqlNodeList = sqlCreateTable.getPropertyList();
        List<SqlNode> property = sqlNodeList.getList();
        tableDescriptor.setProperty(property);
        return new BuilderParseResult(tableDescriptor);
    }

    @Override
    public boolean support(Object sqlNode) {
        if (sqlNode instanceof SqlCreateTable) {
            return true;
        }
        return false;
    }

    @Override
    public CreateSQLBuilder create() {
        return new CreateSQLBuilder();
    }
}
