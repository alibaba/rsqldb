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

import com.alibaba.rsqldb.parser.builder.SelectSqlBuilder;
import com.alibaba.rsqldb.parser.result.IParseResult;
import com.alibaba.rsqldb.parser.result.VarParseResult;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.rocketmq.streams.common.topology.IWindow;
import org.apache.rocketmq.streams.common.utils.StringUtil;

public class HopParser extends TumbleParser {

    public static com.alibaba.rsqldb.parser.builder.WindowBuilder createWindowBuilder(SelectSqlBuilder builder,
        SqlIntervalLiteral size,
        SqlIntervalLiteral slide,
        String timeFieldName) {
        com.alibaba.rsqldb.parser.builder.WindowBuilder
            windowBuilder = new com.alibaba.rsqldb.parser.builder.WindowBuilder();
        windowBuilder.setType(IWindow.HOP_WINDOW);
        windowBuilder.setOwner(builder);
        setWindowParameter(false, windowBuilder, slide);
        setWindowParameter(true, windowBuilder, size);
        windowBuilder.setTimeFieldName(timeFieldName);
        builder.setWindowBuilder(windowBuilder);
        return windowBuilder;
    }

    @Override
    public IParseResult parse(SelectSqlBuilder builder, SqlBasicCall sqlBasicCall) {
        SqlNode[] operands = sqlBasicCall.getOperands();
        String timeFieldName = null;
        if (StringUtil.isNotEmpty(operands[0].toString()) && !"null".equals(operands[0].toString().toLowerCase())) {
            IParseResult fieldName = parseSqlNode(builder, operands[0]);
            timeFieldName = fieldName.getReturnValue();
        }

        SqlIntervalLiteral slide = (SqlIntervalLiteral)operands[1];
        SqlIntervalLiteral size = (SqlIntervalLiteral)operands[2];
        createWindowBuilder(builder, size, slide, timeFieldName);
        return new VarParseResult(null);
    }

    @Override
    public boolean support(Object sqlNode) {
        if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall)sqlNode;
            if (sqlBasicCall.getOperator().getName().toLowerCase().equals("hop")) {
                return true;
            }
        }
        return false;
    }
}
