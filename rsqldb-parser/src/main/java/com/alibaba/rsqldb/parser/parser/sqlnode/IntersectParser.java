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

import com.alibaba.rsqldb.parser.parser.builder.AbstractSqlBuilder;
import com.alibaba.rsqldb.parser.parser.builder.JoinSqlBuilder;
import com.alibaba.rsqldb.parser.parser.builder.SelectSqlBuilder;
import com.alibaba.rsqldb.parser.parser.result.BuilderParseResult;
import com.alibaba.rsqldb.parser.parser.result.IParseResult;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.model.NameCreatorContext;
import org.apache.rocketmq.streams.common.utils.StringUtil;

public class IntersectParser extends AbstractSqlNodeNodeParser<SqlBasicCall, JoinSqlBuilder> {

    private static final Log LOG = LogFactory.getLog(IntersectParser.class);

    @Override
    public IParseResult parse(JoinSqlBuilder joinSQLBuilder, SqlBasicCall sqlBasicCall) {
        joinSQLBuilder.setSqlNode(sqlBasicCall);
        joinSQLBuilder.setJoinType("INNER");
        IParseResult left = doProcessJoinElement(joinSQLBuilder, sqlBasicCall.getOperandList().get(0));
        addConditionField(left,"left_key");
        IParseResult right = doProcessJoinElement(joinSQLBuilder, sqlBasicCall.getOperandList().get(1));
        addConditionField(right,"right_key");
        BuilderParseResult builderParseResult=(BuilderParseResult)right;
        if(StringUtil.isEmpty(builderParseResult.getBuilder().getAsName())){
            builderParseResult.getBuilder().setAsName(NameCreatorContext.get().createNewName(builderParseResult.getBuilder().getTableName()));
        }
        joinSQLBuilder.setLeft(left);
        joinSQLBuilder.setRight(right);
        joinSQLBuilder.setOnCondition("(left_key,==,"+ builderParseResult.getBuilder().getAsName()+".right_key)");
        joinSQLBuilder.setConditionSqlNode(null);
        return new BuilderParseResult(joinSQLBuilder);
    }

    protected void addConditionField(IParseResult parseResult,String joinFieldName) {
        if(!BuilderParseResult.class.isInstance(parseResult)){
            throw new RuntimeException("INTERSECT only support select as SubQuery, the real is "+parseResult.getReturnValue());
        }
        BuilderParseResult builderParseResult=(BuilderParseResult)parseResult;

        AbstractSqlBuilder sqlBuilder=builderParseResult.getBuilder();
        if(!SelectSqlBuilder.class.isInstance(sqlBuilder)){
            throw new RuntimeException("INTERSECT only support select as SubQuery, the real is "+parseResult.getReturnValue());
        }

        SelectSqlBuilder selectSQLBuilder=(SelectSqlBuilder)sqlBuilder;
        selectSQLBuilder.setMsgMd5FieldName(joinFieldName);

       // selectSQLBuilder.getFieldName2ParseResult().put(joinFieldName,new VarParseResult(null));

    }

    @Override
    public JoinSqlBuilder create() {
        return new JoinSqlBuilder();
    }

    @Override
    public boolean support(Object sqlNode) {
        if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
            if (sqlBasicCall.getOperator().getName().toUpperCase().equals("INTERSECT")) {
                return true;
            }
            if (sqlBasicCall.getOperator().getName().toUpperCase().equals("INTERSECT ALL")) {
                return true;
            }
        }
        return false;
    }
}
