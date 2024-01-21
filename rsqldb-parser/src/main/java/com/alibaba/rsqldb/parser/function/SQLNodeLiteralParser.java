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

import com.alibaba.rsqldb.parser.AbstractSqlNodeParser;
import com.alibaba.rsqldb.parser.builder.AbstractSqlBuilder;
import com.alibaba.rsqldb.parser.creator.ParserNameCreator;
import com.alibaba.rsqldb.parser.result.ConstantParseResult;
import com.alibaba.rsqldb.parser.result.IParseResult;
import com.alibaba.rsqldb.parser.result.ScriptParseResult;
import com.alibaba.rsqldb.parser.util.SqlDataTypeUtil;

import org.apache.calcite.sql.SqlLiteral;
import org.apache.rocketmq.streams.common.datatype.DataType;
import org.apache.rocketmq.streams.common.utils.ContantsUtil;

public class SQLNodeLiteralParser extends AbstractSqlNodeParser<SqlLiteral, AbstractSqlBuilder> {

    @Override
    public IParseResult parse(AbstractSqlBuilder tableDescriptor, SqlLiteral sqlNode) {
        String value = sqlNode.toValue();
        if (value == null) {
            ScriptParseResult scriptParseResult = new ScriptParseResult();
            String returnValue = ParserNameCreator.createName("null");
            List<String> scripts = new ArrayList<>();
            scriptParseResult.setScriptValueList(scripts);
            scriptParseResult.setReturnValue(returnValue);
            return scriptParseResult;
        }
        if (value.contains("'")) {
            System.out.println();
        }
        value = value.replace("'", "#######");
        value = ContantsUtil.replaceSpeciaSign(value);
        DataType dataType = SqlDataTypeUtil.covert(sqlNode.getTypeName().getName());
        return new ConstantParseResult(value, dataType);
    }

    @Override
    public boolean support(Object sqlNode) {
        return sqlNode instanceof SqlLiteral;
    }

}
