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

import com.alibaba.rsqldb.parser.parser.SqlNodeParserFactory;
import com.alibaba.rsqldb.parser.parser.builder.AbstractSqlBuilder;
import com.alibaba.rsqldb.parser.parser.builder.InsertSqlBuilder;
import com.alibaba.rsqldb.parser.parser.result.BuilderParseResult;
import com.alibaba.rsqldb.parser.parser.result.IParseResult;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;

public abstract class AbstractInsertParser extends AbstractSqlNodeNodeParser<SqlInsert, InsertSqlBuilder> {

    @Override
    public IParseResult parse(InsertSqlBuilder insertSQLBuilder, SqlInsert sqlInsert) {
        insertSQLBuilder.setTableName(sqlInsert.getTargetTable().toString());
        insertSQLBuilder.setSqlNode(sqlInsert);
        AbstractSqlBuilder sqlDescriptor = null;
        //一般是insert的select部分
        if (sqlInsert.getSource() != null) {
            AbstractSqlNodeNodeParser sqlParser = (AbstractSqlNodeNodeParser) SqlNodeParserFactory.getParse(sqlInsert.getSource());
            sqlDescriptor = sqlParser.create();
            sqlParser.parse(sqlDescriptor, sqlInsert.getSource());
        }
        insertSQLBuilder.setSqlDescriptor(sqlDescriptor);
        SqlNodeList sqlNodeList = sqlInsert.getTargetColumnList();
        if (sqlNodeList != null) {
            List<String> fieldNames = new ArrayList<>();
            List<SqlNode> columnNodes = sqlNodeList.getList();
            if (columnNodes != null) {
                for (SqlNode sqlNode : columnNodes) {
                    IParseResult parseResult = parseSqlNode(insertSQLBuilder, sqlNode);
                    fieldNames.add(parseResult.getReturnValue());
                }
                insertSQLBuilder.setColumnNames(fieldNames);
            }
        }

        parseEmit(insertSQLBuilder, sqlInsert);

        return new BuilderParseResult(insertSQLBuilder);
    }

    protected abstract void parseEmit(InsertSqlBuilder builder, SqlInsert insert) ;

    @Override
    public boolean support(Object sqlNode) {
        if (sqlNode instanceof SqlInsert) {
            return true;
        }
        return false;
    }

    @Override
    public InsertSqlBuilder create() {
        return new InsertSqlBuilder();
    }
}
