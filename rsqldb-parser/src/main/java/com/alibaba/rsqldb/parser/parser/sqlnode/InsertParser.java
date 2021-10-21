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

import com.alibaba.rsqldb.parser.parser.SQLNodeParserFactory;
import com.alibaba.rsqldb.parser.parser.builder.AbstractSQLBuilder;
import com.alibaba.rsqldb.parser.parser.builder.InsertSQLBuilder;
import com.alibaba.rsqldb.parser.parser.result.BuilderParseResult;
import com.alibaba.rsqldb.parser.parser.result.IParseResult;
import org.apache.calcite.sql.SqlInsert;

public class InsertParser extends AbstractSqlNodeParser<SqlInsert, InsertSQLBuilder> {

    @Override
    public IParseResult parse(InsertSQLBuilder tableDescriptor, SqlInsert sqlInsert) {
        tableDescriptor.setTableName(sqlInsert.getTargetTable().toString());
        tableDescriptor.setSqlNode(sqlInsert);
        tableDescriptor.createColumn(sqlInsert.getTargetColumnList());
        AbstractSQLBuilder sqlDescriptor = null;
        //一般是insert的select部分
        if (sqlInsert.getSource() != null) {
            AbstractSqlNodeParser sqlParser = (AbstractSqlNodeParser)SQLNodeParserFactory.getParse(sqlInsert.getSource());
            sqlDescriptor = sqlParser.create();
            sqlParser.parse(sqlDescriptor, sqlInsert.getSource());
        }
        tableDescriptor.setSqlDescriptor(sqlDescriptor);
        return new BuilderParseResult(tableDescriptor);
    }

    @Override
    public boolean support(Object sqlNode) {
        if (sqlNode instanceof SqlInsert) {
            return true;
        }
        return false;
    }

    @Override
    public InsertSQLBuilder create() {
        return new InsertSQLBuilder();
    }
}
