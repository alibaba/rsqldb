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
package com.alibaba.rsqldb.parser.parser.flinksqlnode;

import java.util.Properties;

import com.alibaba.rsqldb.parser.SqlNodeParserFactory;
import com.alibaba.rsqldb.parser.builder.FunctionSqlBuilder;
import com.alibaba.rsqldb.parser.result.BuilderParseResult;
import com.alibaba.rsqldb.parser.result.IParseResult;
import com.alibaba.rsqldb.parser.sqlnode.AbstractSqlNodeNodeParser;

import org.apache.calcite.sql.SqlNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.sql.parser.ddl.SqlCreateFunction;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

/**
 * UDX Parser
 */
public class FunctionParser extends AbstractSqlNodeNodeParser<SqlCreateFunction, FunctionSqlBuilder> {

    private static final Log LOG = LogFactory.getLog(FunctionParser.class);

    @Override
    public IParseResult parse(FunctionSqlBuilder tableDescriptor, SqlCreateFunction sqlCreateFunction) {
        tableDescriptor.setSqlNode(sqlCreateFunction);
        tableDescriptor.setSqlType("function");
        SqlNode functionClassNameNode = sqlCreateFunction.getFunctionClassName();

        String functionClassName = FunctionUtils.getConstant(functionClassNameNode.toString());
        tableDescriptor.setFunctionName(sqlCreateFunction.getFunctionIdentifier()[0].toString());
        tableDescriptor.setClassName(functionClassName);
        if (functionClassName.startsWith("com.lyra.xs.udf.ext.sas_black_rule_v")) {

        }
        SqlNodeParserFactory.addUDF(tableDescriptor.getFunctionName(), tableDescriptor);
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
    public FunctionSqlBuilder create(Properties configuration) {
        FunctionSqlBuilder functionSqlBuilder = new FunctionSqlBuilder();
        functionSqlBuilder.setConfiguration(configuration);
        return functionSqlBuilder;
    }
}