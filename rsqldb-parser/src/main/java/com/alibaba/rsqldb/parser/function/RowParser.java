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
package com.alibaba.rsqldb.parser.function;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.rsqldb.parser.builder.SelectSqlBuilder;
import com.alibaba.rsqldb.parser.result.IParseResult;
import com.alibaba.rsqldb.parser.result.VarParseResult;
import com.alibaba.rsqldb.parser.sqlnode.AbstractSelectNodeParser;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;

public class RowParser extends AbstractSelectNodeParser<SqlBasicCall> {
    @Override
    public IParseResult parse(SelectSqlBuilder builder, SqlBasicCall sqlBasicCall) {
        List<SqlNode> sqlNodeList = sqlBasicCall.getOperandList();
        List<String> fieldNames = new ArrayList<>();
        for (SqlNode sqlNode : sqlNodeList) {
            IParseResult result = parseSqlNode(builder, sqlNode);
            if (result.getReturnValue() != null) {
                fieldNames.add(result.getReturnValue());
            }
        }

        return new VarParseResult(MapKeyUtil.createKey(";", fieldNames));
    }

    @Override
    public boolean support(Object sqlNode) {
        if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall)sqlNode;
            if (sqlBasicCall.getOperator().getName().toUpperCase().equals("ROW")) {
                return true;
            }
        }
        return false;
    }

}
