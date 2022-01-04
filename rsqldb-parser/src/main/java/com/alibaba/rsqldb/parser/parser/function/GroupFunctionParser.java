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

import com.alibaba.rsqldb.parser.parser.builder.SelectSQLBuilder;
import com.alibaba.rsqldb.parser.parser.namecreator.ParserNameCreator;
import com.alibaba.rsqldb.parser.parser.result.IParseResult;
import com.alibaba.rsqldb.parser.parser.result.ScriptParseResult;
import com.alibaba.rsqldb.parser.parser.sqlnode.AbstractSelectNodeParser;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

public class GroupFunctionParser extends AbstractSelectNodeParser<SqlBasicCall> {

    @Override
    public IParseResult parse(SelectSQLBuilder tableDescriptor, SqlBasicCall sqlBasicCall) {
        List<SqlNode> nodeList = sqlBasicCall.getOperandList();
        String name = sqlBasicCall.getOperator().getName().toLowerCase();
        StringBuilder builder = new StringBuilder();
        for (int index = 0; index < nodeList.size(); index++) {
            SqlNode varNode = nodeList.get(index);
            String fieldName = parseSqlNode(tableDescriptor, varNode).getReturnValue();
            builder.append(fieldName);
            if (index != nodeList.size() - 1) {
                builder.append(",");
            }
        }
        ScriptParseResult scriptParseResult = new ScriptParseResult();
        String returnName = ParserNameCreator.createName(name, null);
        String scriptValue = returnName + "=" + name + "(" + builder.toString() + ");";
        scriptParseResult.setReturnValue(returnName);
        scriptParseResult.addScript(tableDescriptor, scriptValue);
        return scriptParseResult;
    }

    @Override
    public boolean support(Object sqlNode) {
        if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall)sqlNode;
            String name = sqlBasicCall.getOperator().getName().toLowerCase();
            if (name.equals("sum")) {
                return true;
            }
            if (name.equals("max")) {
                return true;
            }
            if (name.equals("min")) {
                return true;
            }
            if (name.equals("avg")) {
                return true;
            }
            if ("concat_distinct".equals(name)) {
                return true;
            }
            if ("concat_agg".equals(name)) {
                return true;
            }
        }
        return false;
    }
}
