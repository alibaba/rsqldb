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
import java.util.List;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;

public class LowerParser extends AbstractSelectNodeParser<SqlBasicCall> {

    @Override
    public IParseResult parse(SelectSqlBuilder tableDescriptor, SqlBasicCall sqlBasicCall) {
        List<SqlNode> nodeList = sqlBasicCall.getOperandList();
        SqlNode varNode = nodeList.get(0);
        String varName = parseSqlNode(tableDescriptor, varNode).getReturnValue();
        ScriptParseResult scriptParseResult = new ScriptParseResult();
        String functionName = "lower";
        String returnName = tableDescriptor.getInnerVarName(functionName, varName);

        if (returnName == null) {
            returnName = ParserNameCreator.createName(functionName, varName);
            tableDescriptor.putInnerVarName(functionName, varName, returnName);
            String scriptValue = returnName + "=lower(" + varName + ");";
            scriptParseResult.addScript(tableDescriptor, scriptValue);
        }
        scriptParseResult.setReturnValue(returnName);
        return scriptParseResult;
    }

    @Override
    public boolean support(Object sqlNode) {
        if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
            if (("lower").equals(sqlBasicCall.getOperator().getName().toLowerCase())) {
                return true;
            }
        }
        return false;
    }
}
