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

import com.alibaba.rsqldb.parser.parser.ISqlParser;
import com.alibaba.rsqldb.parser.parser.SQLNodeParserFactory;
import com.alibaba.rsqldb.parser.parser.builder.AbstractSQLBuilder;
import com.alibaba.rsqldb.parser.parser.builder.JoinSQLBuilder;
import com.alibaba.rsqldb.parser.parser.builder.LateralTableBuilder;
import com.alibaba.rsqldb.parser.parser.builder.SelectSQLBuilder;
import com.alibaba.rsqldb.parser.parser.builder.TableNodeBuilder;
import com.alibaba.rsqldb.parser.parser.function.SqlIndentifierParser;
import com.alibaba.rsqldb.parser.parser.result.BuilderParseResult;
import com.alibaba.rsqldb.parser.parser.result.IParseResult;
import com.alibaba.rsqldb.parser.parser.result.ScriptParseResult;
import java.util.List;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class JoinParser extends AbstractSqlNodeParser<SqlJoin, JoinSQLBuilder> {

    private static final Log LOG = LogFactory.getLog(JoinParser.class);

    @Override
    public IParseResult parse(JoinSQLBuilder joinSQLBuilder, SqlJoin sqlJoin) {
        joinSQLBuilder.setSqlNode(sqlJoin);
        joinSQLBuilder.setJoinType(sqlJoin.getJoinType().toString());
        IParseResult left = doProcessJoinElement(joinSQLBuilder, sqlJoin.getLeft());

        joinSQLBuilder.setLeft(left);

        IParseResult rigth = doProcessJoinElement(joinSQLBuilder, sqlJoin.getRight());
        joinSQLBuilder.setRight(rigth);
        SqlNode sqlNode = sqlJoin.getCondition();
        if (sqlNode == null || "true".equals(sqlNode.toString().toLowerCase())) {
            if (BuilderParseResult.class.isInstance(left) && BuilderParseResult.class.isInstance(rigth)) {
                BuilderParseResult leftBuilder = (BuilderParseResult) left;
                BuilderParseResult rigthBuilder = (BuilderParseResult) rigth;
                if (TableNodeBuilder.class.isInstance(leftBuilder.getBuilder()) && TableNodeBuilder.class.isInstance(rigthBuilder.getBuilder())) {
                    joinSQLBuilder.setNeedWhereToCondition(true);
                }
            }
            return new BuilderParseResult(joinSQLBuilder);
        }
        ISqlParser sqlParser = SQLNodeParserFactory.getParse(sqlNode);
        boolean isSelectSwitch = joinSQLBuilder.isSelectStage();
        boolean isFromSwitch = joinSQLBuilder.isFromStage();
        joinSQLBuilder.switchWhere();
        IParseResult result = sqlParser.parse(joinSQLBuilder, sqlNode);
        if (isFromSwitch) {
            joinSQLBuilder.switchFrom();
        }
        if (isSelectSwitch) {
            joinSQLBuilder.switchSelect();
        }
        joinSQLBuilder.setOnCondition(result.getValueForSubExpression());
        joinSQLBuilder.setConditionSqlNode(sqlNode);
        return new BuilderParseResult(joinSQLBuilder);
    }

    protected IParseResult doProcessJoinElement(JoinSQLBuilder descriptor, SqlNode sqlNode) {

        ISqlParser parse = SQLNodeParserFactory.getParse(sqlNode);

        /**
         * 单表名
         */
        if (parse instanceof SqlIndentifierParser) {
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
                AbstractSqlNodeParser sqlParser = (AbstractSqlNodeParser) SQLNodeParserFactory.getParse(node);
                LateralTableBuilder lateralTableBuilder = new LateralTableBuilder();
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
                    }else {
                        lateralTableBuilder.addDefaultFields();
                    }

                }
                return builderParseResult;
            }
        }
        /**
         * 是 select 作为表的场景
         */
        AbstractSqlNodeParser sqlParser = (AbstractSqlNodeParser) SQLNodeParserFactory.getParse(node);
        if (sqlParser != null) {
            AbstractSQLBuilder sqlDescriptor = sqlParser.create();
            IParseResult parseResult = sqlParser.parse(sqlDescriptor, node);
            if (SelectSQLBuilder.class.isInstance(sqlDescriptor)) {
                SelectSQLBuilder selectSQLBuilder = (SelectSQLBuilder) sqlDescriptor;
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
                descriptor.addScript(scriptParseResult.getScript());
                return parseResult;
            } else {

                return parseResult;
            }
        }
        throw new RuntimeException("can not find parser,the paser data is " + sqlNode.toString());
    }

    @Override
    public JoinSQLBuilder create() {
        return new JoinSQLBuilder();
    }

    @Override
    public boolean support(Object sqlNode) {
        return sqlNode instanceof SqlJoin;
    }
}
