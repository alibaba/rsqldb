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
package com.alibaba.rsqldb.parser.sqlnode;

import java.util.Properties;

import com.alibaba.rsqldb.parser.ISqlNodeParser;
import com.alibaba.rsqldb.parser.SqlNodeParserFactory;
import com.alibaba.rsqldb.parser.builder.JoinSqlBuilder;
import com.alibaba.rsqldb.parser.builder.TableNodeBuilder;
import com.alibaba.rsqldb.parser.result.BuilderParseResult;
import com.alibaba.rsqldb.parser.result.IParseResult;

import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class JoinParser extends AbstractSqlNodeNodeParser<SqlJoin, JoinSqlBuilder> {

    private static final Log LOG = LogFactory.getLog(JoinParser.class);

    @Override
    public IParseResult parse(JoinSqlBuilder joinSQLBuilder, SqlJoin sqlJoin) {
        joinSQLBuilder.setSqlNode(sqlJoin);
        joinSQLBuilder.setJoinType(sqlJoin.getJoinType().toString());
        IParseResult left = doProcessJoinElement(joinSQLBuilder, sqlJoin.getLeft());

        joinSQLBuilder.setLeft(left);

        IParseResult rigth = doProcessJoinElement(joinSQLBuilder, sqlJoin.getRight());
        joinSQLBuilder.setRight(rigth);
        SqlNode sqlNode = sqlJoin.getCondition();
        if (sqlNode == null || "true".equalsIgnoreCase(sqlNode.toString())) {
            if (left instanceof BuilderParseResult && rigth instanceof BuilderParseResult) {
                BuilderParseResult leftBuilder = (BuilderParseResult)left;
                BuilderParseResult rigthBuilder = (BuilderParseResult)rigth;
                if (leftBuilder.getBuilder() instanceof TableNodeBuilder && rigthBuilder.getBuilder() instanceof TableNodeBuilder) {
                    joinSQLBuilder.setNeedWhereToCondition(true);
                }
            }
            return new BuilderParseResult(joinSQLBuilder);
        }
        ISqlNodeParser sqlParser = SqlNodeParserFactory.getParse(sqlNode);
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
        return new BuilderParseResult(joinSQLBuilder);
    }

    @Override
    public JoinSqlBuilder create(Properties configuration) {
        JoinSqlBuilder joinSqlBuilder = new JoinSqlBuilder();
        joinSqlBuilder.setConfiguration(configuration);
        return joinSqlBuilder;
    }

    @Override
    public boolean support(Object sqlNode) {
        return sqlNode instanceof SqlJoin;
    }
}
