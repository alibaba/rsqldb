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

import com.alibaba.rsqldb.parser.parser.builder.SelectSqlBuilder;
import com.alibaba.rsqldb.parser.parser.result.IParseResult;
import com.alibaba.rsqldb.parser.parser.result.ScriptParseResult;
import com.alibaba.rsqldb.parser.parser.sqlnode.AbstractSelectNodeParser;
import java.util.List;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;

public class IsTrueFunction extends AbstractSelectNodeParser<SqlBasicCall> {

    @Override
    public IParseResult parse(SelectSqlBuilder tableDescriptor, SqlBasicCall sqlBasicCall) {
        List<SqlNode> nodeList = sqlBasicCall.getOperandList();
        boolean isWhereStage = tableDescriptor.isWhereStage();
        if (isWhereStage) {
            tableDescriptor.switchSelect();
        }
        IParseResult valueSqlVar = parseSqlNode(tableDescriptor, nodeList.get(0));
        if (isWhereStage) {
            tableDescriptor.switchWhere();
        }
        String varName = valueSqlVar.getReturnValue();
        ScriptParseResult sqlVar = null;
        sqlVar = new ScriptParseResult();
        sqlVar.addScript("(" + varName + ",==,true)");
        return sqlVar;
    }

    @Override
    public boolean support(Object sqlNode) {
        if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
            if (sqlBasicCall.getOperator().getName().toLowerCase().equals("is true")) {
                return true;
            }
            if (sqlBasicCall.getOperator().getName().toLowerCase().equals("is not false")) {
                return true;
            }

        }
        return false;
    }
}
