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

import com.alibaba.rsqldb.parser.parser.builder.JoinConditionSQLBuilder;
import com.alibaba.rsqldb.parser.parser.builder.JoinSQLBuilder;
import com.alibaba.rsqldb.parser.parser.builder.SelectSQLBuilder;
import com.alibaba.rsqldb.parser.parser.namecreator.ParserNameCreator;
import com.alibaba.rsqldb.parser.parser.result.ConstantParseResult;
import com.alibaba.rsqldb.parser.parser.result.IParseResult;
import com.alibaba.rsqldb.parser.parser.result.ScriptParseResult;
import com.alibaba.rsqldb.parser.parser.result.VarParseResult;
import com.alibaba.rsqldb.parser.parser.sqlnode.AbstractSelectNodeParser;
import java.util.List;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.rocketmq.streams.common.datatype.DataType;
import org.apache.rocketmq.streams.common.datatype.StringDataType;
import org.apache.rocketmq.streams.filter.function.expression.CompareFunction;

public class CompareParser extends AbstractSelectNodeParser<SqlBasicCall> {

    @Override
    public IParseResult parse(SelectSQLBuilder tableDescriptor, SqlBasicCall sqlBasicCall) {
        List<SqlNode> nodeList = sqlBasicCall.getOperandList();
        String functionName = "==";
        if (sqlBasicCall.getOperator().getName().equals("=")) {
            functionName = "==";
        } else {
            functionName = sqlBasicCall.getOperator().getName();
        }

        IParseResult varSqlVar = parseSqlNode(tableDescriptor, nodeList.get(0));

        IParseResult valueSqlVar = parseSqlNode(tableDescriptor, nodeList.get(1));
        if (!JoinSQLBuilder.class.isInstance(tableDescriptor) && !JoinConditionSQLBuilder.class.isInstance(tableDescriptor) && VarParseResult.class.isInstance(valueSqlVar) && tableDescriptor.isWhereStage()) {
            /**
             * 以前的逻辑会把值当常量，为了兼容变量，加前缀的目的是为了标识这个值是变量，在CompareFunction会根据这个标识对变量做特殊处理
             */
            ((VarParseResult) valueSqlVar).setValue(CompareFunction.VAR_PREFIX + valueSqlVar.getResultValue());
        }
        if (tableDescriptor.isWhereStage() && varSqlVar instanceof ScriptParseResult
            && varSqlVar.getReturnValue() == null) {
            if (valueSqlVar.getReturnValue().toLowerCase().equals("true")) {
                return varSqlVar;
            }
            if (valueSqlVar.getReturnValue().toLowerCase().equals("false")) {
                ScriptParseResult scriptParseResult = new ScriptParseResult();
                String varName = ParserNameCreator.createName("!", null);
                String scriptValue = varSqlVar.getValueForSubExpression();
                tableDescriptor.addScript(varName + "=!(" + scriptValue + ")");
                scriptParseResult.addScript(varName);
                return scriptParseResult;
            }

        }
        DataType dataType = null;
        if (varSqlVar instanceof ConstantParseResult) {
            ConstantParseResult constantPaseResult = (ConstantParseResult) varSqlVar;
            dataType = constantPaseResult.getDataType();
        }
        String varName = null;
        if (dataType == null || StringDataType.getTypeName().equals(dataType.getDataTypeName())) {
            varName = varSqlVar.getReturnValue();

        } else {
            varName = valueSqlVar.getReturnValue();
            valueSqlVar = varSqlVar;
            if (functionName.equals(">")) {
                functionName = "<";
            } else if (functionName.equals(">=")) {
                functionName = "<=";
            } else if (functionName.equals("<")) {
                functionName = ">";
            } else if (functionName.equals("<=")) {
                functionName = ">=";
            }
        }
        if (tableDescriptor.isSelectStage()) {
            if (functionName.equals("==")) {
                functionName = "equals";
            } else if (functionName.equals(">=")) {
                functionName = "greateeq";
            } else if (functionName.equals("<=")) {
                functionName = "lesseq";
            } else if (functionName.equals("<")) {
                functionName = "less";
            } else if (functionName.equals(">")) {
                functionName = "great";
            } else if (functionName.equals("<>") || functionName.equals("!=")) {
                functionName = "!equals";
            }
            String returnName = ParserNameCreator.createName(functionName);
            String script = returnName + "=" + functionName + "(" + varName + "," + valueSqlVar
                .getValueForSubExpression() + ");";
            ScriptParseResult scriptParseResult = new ScriptParseResult();
            scriptParseResult.setReturnValue(returnName);
            scriptParseResult.addScript(tableDescriptor, script);
            return scriptParseResult;
        }
        return createExpression(varName, functionName, valueSqlVar);
    }

    @Override
    public boolean support(Object sqlNode) {
        if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
            if (sqlBasicCall.getOperator().getName().equals("=")) {
                return true;
            }
            if (sqlBasicCall.getOperator().getName().equals("<>")) {
                return true;
            }
            if (sqlBasicCall.getOperator().getName().equals("!=")) {
                return true;
            }
            if (sqlBasicCall.getOperator().getName().equals(">=")) {
                return true;
            }
            if (sqlBasicCall.getOperator().getName().equals("<=")) {
                return true;
            }
            if (sqlBasicCall.getOperator().getName().equals(">")) {
                return true;
            }
            if (sqlBasicCall.getOperator().getName().equals("<")) {
                return true;
            }
        }
        return false;
    }
}
