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
package com.alibaba.rsqldb.parser.parser.function;

import com.alibaba.rsqldb.parser.parser.builder.SelectSqlBuilder;
import com.alibaba.rsqldb.parser.creator.ParserNameCreator;
import com.alibaba.rsqldb.parser.parser.result.IParseResult;
import com.alibaba.rsqldb.parser.parser.result.ScriptParseResult;
import com.alibaba.rsqldb.parser.parser.sqlnode.AbstractSelectNodeParser;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;

public class TrimFunctionParser extends AbstractSelectNodeParser<SqlBasicCall> {

    @Override
    public IParseResult parse(SelectSqlBuilder builder, SqlBasicCall sqlBasicCall) {
        SqlNode sqlNode = sqlBasicCall.getOperandList().get(2);
        String value = parseSqlNode(builder, sqlNode).getValueForSubExpression();
        ScriptParseResult scriptParseResult = new ScriptParseResult();
        String functionName = "trim";
        String returnName = builder.getInnerVarName(functionName, value);
        if (returnName == null) {
            returnName = ParserNameCreator.createName(functionName);
            builder.putInnerVarName(functionName, value, returnName);
            scriptParseResult.addScript(builder, returnName + "=trim(" + value + ");");
        }

        scriptParseResult.setReturnValue(returnName);
        return scriptParseResult;
    }

    @Override
    public boolean support(Object sqlNode) {
        if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
            if (sqlBasicCall.getOperator().getName().toLowerCase().equals("trim")) {
                return true;
            }
        }
        return false;
    }
}
