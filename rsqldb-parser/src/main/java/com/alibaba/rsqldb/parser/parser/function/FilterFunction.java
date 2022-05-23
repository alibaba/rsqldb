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
import com.alibaba.rsqldb.parser.parser.result.IParseResult;
import com.alibaba.rsqldb.parser.parser.result.ScriptParseResult;
import com.alibaba.rsqldb.parser.parser.sqlnode.AbstractSelectNodeParser;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;

public class FilterFunction extends AbstractSelectNodeParser<SqlBasicCall> {

    @Override
    public IParseResult parse(SelectSQLBuilder selectSQLBuilder, SqlBasicCall sqlBasicCall) {
        List<SqlNode> nodeList = sqlBasicCall.getOperandList();
        if (nodeList.size() != 2) {
            throw new RuntimeException("can not support parser the node " + sqlBasicCall);
        }
        SqlNode condition = nodeList.get(1);
        SqlNode action = nodeList.get(0);

        String varName = parseSqlNode(selectSQLBuilder, condition).getReturnValue();
        List<String> scriptList = new ArrayList<>(selectSQLBuilder.getScripts());
        IParseResult result = parseSqlNode(selectSQLBuilder, action);
        if (ScriptParseResult.class.isInstance(result)) {
            ScriptParseResult scriptParseResult = (ScriptParseResult) result;
            String script = "if(" + varName + "){" + scriptParseResult.getScript() + "};";
            scriptList.add(script);
            selectSQLBuilder.setScripts(scriptList);
            return scriptParseResult;
        }
        throw new RuntimeException("can not support parser the node " + sqlBasicCall);
    }

    @Override
    public boolean support(Object sqlNode) {
        if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
            if (sqlBasicCall.getOperator().getName().toLowerCase().equals("filter")) {
                return true;
            }
        }
        return false;
    }
}
