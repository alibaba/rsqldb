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

import com.alibaba.rsqldb.parser.parser.AbstractSqlNodeParser;
import com.alibaba.rsqldb.parser.parser.ISqlNodeParser;
import com.alibaba.rsqldb.parser.parser.SqlNodeParserFactory;
import com.alibaba.rsqldb.parser.parser.builder.AbstractSqlBuilder;
import com.alibaba.rsqldb.parser.parser.builder.JoinSqlBuilder;
import com.alibaba.rsqldb.parser.parser.builder.LateralTableBuilder;
import com.alibaba.rsqldb.parser.parser.builder.SelectSqlBuilder;
import com.alibaba.rsqldb.parser.parser.builder.TableNodeBuilder;
import com.alibaba.rsqldb.parser.parser.function.SQLNodeIndentifierParser;
import com.alibaba.rsqldb.parser.parser.result.BuilderParseResult;
import com.alibaba.rsqldb.parser.parser.result.IParseResult;
import com.alibaba.rsqldb.parser.parser.result.ScriptParseResult;
import java.util.List;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;

public abstract class AbstractSqlNodeNodeParser<T, DESCRIPTOR extends AbstractSqlBuilder>
    extends AbstractSqlNodeParser<T, DESCRIPTOR> implements
    IBuilderCreator<DESCRIPTOR> {

    protected IParseResult doProcessJoinElement(JoinSqlBuilder joinSQLBuilder, SqlNode sqlNode) {

        ISqlNodeParser parse = SqlNodeParserFactory.getParse(sqlNode);

        /**
         * 单表名
         */
        if (parse instanceof SQLNodeIndentifierParser) {
            TableNodeBuilder tableNodeBuilder = new TableNodeBuilder();
            tableNodeBuilder.setTableName(sqlNode.toString());
            return new BuilderParseResult(tableNodeBuilder);
        }

        SqlNode node = sqlNode;
        String asName = null;
        List<SqlNode> nodeList = null;
        /**
         * 单表名+ as
         */
        if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
            if (sqlBasicCall.getOperator().getName().toLowerCase().equals("as")) {
                nodeList = sqlBasicCall.getOperandList();
                node = nodeList.get(0);
                asName = nodeList.get(1).toString();
            }
        }

        if (node instanceof SqlIdentifier) {
            TableNodeBuilder tableNodeBuilder = new TableNodeBuilder();
            tableNodeBuilder.setTableName(node.toString());
            tableNodeBuilder.setAsName(asName);
            return new BuilderParseResult(tableNodeBuilder);
        }
        if (node instanceof SqlBasicCall) {
            SqlBasicCall basicCall = (SqlBasicCall) node;
            if (basicCall.getOperator().getName().toLowerCase().equals("lateral")) {
                AbstractSqlNodeNodeParser sqlParser = (AbstractSqlNodeNodeParser) SqlNodeParserFactory.getParse(node);
                LateralTableBuilder lateralTableBuilder = new LateralTableBuilder();
                lateralTableBuilder.setSqlNode(node);
                IParseResult result = sqlParser.parse(lateralTableBuilder, node);
                BuilderParseResult builderParseResult = new BuilderParseResult(lateralTableBuilder);
                builderParseResult.getBuilder().setAsName(asName);
                /**
                 * udtf场景，针对tateral table（） as t(a,b)括号中的名字做处理
                 */
                if (LateralTableBuilder.class.isInstance(builderParseResult.getBuilder())) {
                    lateralTableBuilder = (LateralTableBuilder) builderParseResult.getBuilder();
                    if (nodeList.size() > 2) {

                        lateralTableBuilder.addFields(nodeList);
                    } else {
                        lateralTableBuilder.addDefaultFields();
                    }

                }
                return builderParseResult;
            }
        }
        /**
         * 是 select 作为表的场景
         */
        AbstractSqlNodeNodeParser sqlParser = (AbstractSqlNodeNodeParser) SqlNodeParserFactory.getParse(node);
        if (sqlParser != null) {
            AbstractSqlBuilder sqlDescriptor = sqlParser.create();
            IParseResult parseResult = sqlParser.parse(sqlDescriptor, node);
            if (SelectSqlBuilder.class.isInstance(sqlDescriptor)) {
                SelectSqlBuilder selectSQLBuilder = (SelectSqlBuilder) sqlDescriptor;
                selectSQLBuilder.setAsName(asName);
            }
            // builder.addDependentTable(parseResult.getReturnValue());
            //            builder.setTableName(parseResult.getReturnValue());
            if (parseResult instanceof BuilderParseResult) {

                return parseResult;
            } else if (sqlDescriptor.getSqlNode() != null && sqlDescriptor.getSqlNode() instanceof SqlSelect) {
                sqlDescriptor.setAsName(asName);
                return new BuilderParseResult(sqlDescriptor);
            } else if (parseResult instanceof ScriptParseResult) {
                ScriptParseResult scriptParseResult = (ScriptParseResult) parseResult;
                joinSQLBuilder.addScript(scriptParseResult.getScript());
                return parseResult;
            } else {

                return parseResult;
            }
        }
        throw new RuntimeException("can not find parser,the paser data is " + sqlNode.toString());
    }

}
