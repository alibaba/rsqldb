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

import org.apache.rocketmq.streams.common.datatype.DataType;
import com.alibaba.rsqldb.parser.parser.builder.SelectSQLBuilder;
import com.alibaba.rsqldb.parser.parser.namecreator.ParserNameCreator;
import com.alibaba.rsqldb.parser.parser.result.IParseResult;
import com.alibaba.rsqldb.parser.parser.result.ScriptParseResult;
import com.alibaba.rsqldb.parser.parser.sqlnode.AbstractSelectNodeParser;
import com.alibaba.rsqldb.parser.util.SqlDataTypeUtil;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;

public class CastFunctionParser extends AbstractSelectNodeParser<SqlBasicCall> {

    @Override
    public IParseResult parse(SelectSQLBuilder tableDescriptor, SqlBasicCall sqlBasicCall) {

        List<SqlNode> nodeList = sqlBasicCall.getOperandList();
        SqlNode varNode = nodeList.get(0);
        SqlNode typeNode = nodeList.get(1);
        String varName = parseSqlNode(tableDescriptor, varNode).getReturnValue();

        String typeValue = parseSqlNode(tableDescriptor, typeNode).getReturnValue();
        DataType dataType = SqlDataTypeUtil.covert(typeValue);
        String returnName = ParserNameCreator.createName("cast", null);
        String scriptValue = returnName + "=cast(" + varName + ",'" + dataType.getDataTypeName() + "');";
        ScriptParseResult scriptParseResult = new ScriptParseResult();
        scriptParseResult.setReturnValue(returnName);
        scriptParseResult.addScript(tableDescriptor, scriptValue);
        return scriptParseResult;
    }

    @Override
    public boolean support(Object sqlNode) {
        if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall)sqlNode;
            if (sqlBasicCall.getOperator().getName().toLowerCase().equals("cast")) {
                return true;
            }
        }
        return false;
    }
}
