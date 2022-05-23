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
package com.alibaba.rsqldb.parser.parser.expression;

import com.alibaba.rsqldb.parser.parser.builder.SelectSQLBuilder;
import com.alibaba.rsqldb.parser.parser.result.ConstantParseResult;
import com.alibaba.rsqldb.parser.parser.result.IParseResult;
import com.alibaba.rsqldb.parser.parser.result.ScriptParseResult;
import com.alibaba.rsqldb.parser.parser.sqlnode.AbstractSelectNodeParser;
import java.util.List;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlLikeOperator;

public class LikeFunction extends AbstractSelectNodeParser<SqlBasicCall> {

    @Override
    public IParseResult parse(SelectSQLBuilder tableDescriptor, SqlBasicCall sqlBasicCall) {
        String functionName = sqlBasicCall.getOperator().getName().toLowerCase().equals("like") ? "like" : "!like";
        List<SqlNode> nodeList = sqlBasicCall.getOperandList();
        String varName = parseSqlNode(tableDescriptor, nodeList.get(0)).getReturnValue();
        IParseResult valueSqlVar = parseSqlNode(tableDescriptor, nodeList.get(1));
        String value = valueSqlVar.getValueForSubExpression();
        ScriptParseResult sqlVar = null;
        if (ConstantParseResult.class.isInstance(valueSqlVar) && valueSqlVar.isConstantString() == false) {
            ConstantParseResult constantParseResult = (ConstantParseResult) valueSqlVar;
            sqlVar = new ScriptParseResult();
            sqlVar.addScript(
                "(" + varName + "," + functionName + "," + constantParseResult.getDataType().getDataTypeName() + ","
                    + value + ")");
        } else {
            sqlVar = new ScriptParseResult();
            sqlVar.addScript("(" + varName + "," + functionName + "," + value + ")");
        }
        return sqlVar;
    }

    @Override
    public boolean support(Object sqlNode) {
        if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
            if (sqlBasicCall.getOperator() instanceof SqlLikeOperator) {
                return true;
            }

        }
        return false;
    }
}
