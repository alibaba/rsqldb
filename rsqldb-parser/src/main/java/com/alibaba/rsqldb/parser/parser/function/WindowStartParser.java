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

/**
 * tumble or hop window's start time
 */
public class WindowStartParser extends AbstractSelectNodeParser<SqlBasicCall> {

    String tumbleFunction = "tumble_start";

    String hopFunction = "hop_start";

    String sessionFunction = "session_start";

    @Override
    public IParseResult parse(SelectSQLBuilder builder, SqlBasicCall sqlBasicCall) {
        ScriptParseResult scriptParseResult = new ScriptParseResult();
        String returnName = ParserNameCreator.createName("window_start");
        scriptParseResult.addScript(builder, returnName + "=window_start();");
        scriptParseResult.setReturnValue(returnName);
        return scriptParseResult;
    }

    @Override
    public boolean support(Object sqlNode) {
        if (SqlBasicCall.class.isInstance(sqlNode)) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
            String name = sqlBasicCall.getOperator().getName().toLowerCase();
            if (tumbleFunction.equals(name) || hopFunction.equals(name) || sessionFunction.equals(name)) {
                return true;
            }
        }
        return false;
    }

}
