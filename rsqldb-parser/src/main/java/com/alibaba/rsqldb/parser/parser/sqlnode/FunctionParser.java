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
package com.alibaba.rsqldb.parser.parser.sqlnode;

import com.alibaba.rsqldb.parser.parser.SQLNodeParserFactory;
import com.alibaba.rsqldb.parser.parser.builder.FunctionSQLBuilder;
import com.alibaba.rsqldb.parser.parser.result.BuilderParseResult;
import com.alibaba.rsqldb.parser.parser.result.IParseResult;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.sql.parser.ddl.SqlCreateFunction;

/**
 * UDX Parser
 */
public class FunctionParser extends AbstractSqlNodeParser<SqlCreateFunction, FunctionSQLBuilder> {

    private static final Log LOG = LogFactory.getLog(FunctionParser.class);

    @Override
    public IParseResult parse(FunctionSQLBuilder tableDescriptor, SqlCreateFunction sqlCreateFunction) {
        tableDescriptor.setSqlNode(sqlCreateFunction);
        tableDescriptor.setSqlType("function");
        tableDescriptor.setFunctionName(sqlCreateFunction.getFunctionName().toString());
        tableDescriptor.setClassName(sqlCreateFunction.getClassName());
        if(sqlCreateFunction.getClassName().startsWith("com.lyra.xs.udf.ext.sas_black_rule_v")){

        }
        SQLNodeParserFactory.addUDF(tableDescriptor.getFunctionName(),tableDescriptor);
        return new BuilderParseResult(tableDescriptor);
    }

    @Override
    public boolean support(Object sqlNode) {
        if (sqlNode instanceof SqlCreateFunction) {
            return true;
        }
        return false;
    }

    @Override
    public FunctionSQLBuilder create() {
        return new FunctionSQLBuilder();
    }
}
