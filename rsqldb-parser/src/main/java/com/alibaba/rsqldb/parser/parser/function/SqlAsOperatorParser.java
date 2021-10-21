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

import java.util.List;

import com.alibaba.rsqldb.parser.parser.builder.SelectSQLBuilder;
import com.alibaba.rsqldb.parser.parser.result.IParseResult;
import com.alibaba.rsqldb.parser.parser.result.NotSupportParseResult;
import com.alibaba.rsqldb.parser.parser.result.ScriptParseResult;
import com.alibaba.rsqldb.parser.parser.sqlnode.AbstractSelectNodeParser;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SqlAsOperatorParser extends AbstractSelectNodeParser<SqlBasicCall> {

    private static final Log LOG = LogFactory.getLog(SqlAsOperatorParser.class);

    @Override
    public IParseResult parse(SelectSQLBuilder tableDescriptor, SqlBasicCall sqlBasicCall) {
        return parse(tableDescriptor, sqlBasicCall.getOperandList());
    }

    protected IParseResult parse(SelectSQLBuilder tableDescriptor, List<SqlNode> nodeList) {
        SqlNode sqlNode = nodeList.get(0);
        String varName = nodeList.get(1).toString();
        IParseResult valueResult = parseSqlNode(tableDescriptor, sqlNode);
        if (valueResult instanceof NotSupportParseResult) {
            return valueResult;
        }

        String value = valueResult.getValueForSubExpression();
        if (value == null) {
            throw new RuntimeException(tableDescriptor.getTableName() + "." + sqlNode + " can not find field from "
                + tableDescriptor.getTableName() + " table。the sql is " + tableDescriptor.getSqlNode().toString());
        }
        if(tableDescriptor.getOverName()!=null&&value.equals(tableDescriptor.getOverName())){
            tableDescriptor.setOverName(varName);
        }
        String scriptValue = varName + "=" + value + ";";
        if (value.startsWith("__")) {
            scriptValue = scriptValue + "rm('" + value + "');";
        }

        ScriptParseResult scriptParseResult = new ScriptParseResult();
        scriptParseResult.setReturnValue(varName);
        scriptParseResult.addScript(tableDescriptor, scriptValue);

        return scriptParseResult;
    }

    @Override
    public boolean support(Object sqlNode) {
        if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall)sqlNode;
            if (sqlBasicCall.getOperator().getName().toLowerCase().equals("as")) {
                return true;
            }
        }
        if (sqlNode instanceof SqlAsOperator) {
            return true;
        }
        return false;
    }
}
