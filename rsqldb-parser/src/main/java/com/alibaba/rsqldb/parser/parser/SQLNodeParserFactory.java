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
package com.alibaba.rsqldb.parser.parser;

import com.alibaba.rsqldb.parser.parser.builder.AbstractSQLBuilder;
import com.alibaba.rsqldb.parser.parser.builder.FunctionSQLBuilder;
import com.alibaba.rsqldb.parser.parser.builder.SelectSQLBuilder;
import com.alibaba.rsqldb.parser.parser.result.IParseResult;
import com.alibaba.rsqldb.parser.parser.result.ScriptParseResult;
import com.alibaba.rsqldb.parser.parser.sqlnode.IBuilderCreator;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.collections.map.HashedMap;
import org.apache.rocketmq.streams.common.calssscaner.AbstractScan;
import org.apache.rocketmq.streams.common.model.NameCreatorContext;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;

/**
 * 可以通过扩展来增加sql解析能力，主体框架不变化
 */
public class SQLNodeParserFactory {

    /**
     * sql parser name and it's parser
     */
    private static final Map<String, ISqlParser> sqlParsers = new HashMap<>();

    /**
     * 保存解析的udf
     */
    private static Map<String, FunctionSQLBuilder> udfSet = new HashedMap();

    private static AbstractScan scan = new AbstractScan() {
        @Override
        protected void doProcessor(Class clazz, String functionName) {
            if (ISqlParser.class.isAssignableFrom(clazz)) {
                if (Modifier.isAbstract(clazz.getModifiers())) {
                    return;
                }
                register(ReflectUtil.forInstance(clazz));
            }
        }
    };

    static {
        scan.scanPackage("com.alibaba.rsqldb.parser.parser.sqlnode");
        scan.scanPackage("com.alibaba.rsqldb.parser.parser.expression");
        scan.scanPackage("com.alibaba.rsqldb.parser.parser.function");

    }

    /**
     * 注册sql解析器
     *
     * @param sqlParser
     */
    protected static void register(ISqlParser sqlParser) {
        if (sqlParser == null) {
            return;
        }
        sqlParsers.put(sqlParser.getClass().getSimpleName(), sqlParser);
    }

    /**
     * 获取对应的解析器
     *
     * @param sqlNode
     * @return
     */
    public static ISqlParser getParse(Object sqlNode) {
        for (ISqlParser<?, ?> sqlParser : sqlParsers.values()) {
            if (sqlParser.support(sqlNode)) {
                return sqlParser;
            }
        }
        if (sqlNode instanceof SqlBasicCall) {
            final SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
            String name = sqlBasicCall.getOperator().getName().toUpperCase();
            if (udfsContains(name)) {
                return new ISqlParser() {

                    @Override
                    public IParseResult parse(AbstractSQLBuilder builder, Object object) {
                        if (!(builder instanceof SelectSQLBuilder)) {
                            throw new RuntimeException("can not support parser udf " + name + ", expect select sql builder ,real is " + builder.getClass().getName());
                        }
                        String returnValue = NameCreatorContext.get().createNewName("__", name);
                        String scriptValue = returnValue + "=" + name + "(";
                        boolean isFirst = true;
                        List<SqlNode> nodeList = sqlBasicCall.getOperandList();
                        if (nodeList != null) {
                            for (SqlNode node : nodeList) {
                                if (isFirst) {
                                    isFirst = false;
                                } else {
                                    scriptValue = scriptValue + ",";
                                }
                                scriptValue = scriptValue + getParse(node).parse(builder, node).getValueForSubExpression();
                            }
                        }
                        scriptValue = scriptValue + ");";
                        ScriptParseResult scriptParseResult = new ScriptParseResult();
                        scriptParseResult.addScript((SelectSQLBuilder) builder, scriptValue);
                        scriptParseResult.setReturnValue(returnValue);
                        return scriptParseResult;
                    }

                    @Override
                    public boolean support(Object sqlNode) {
                        return true;
                    }
                };
            }
        }
        return null;
    }

    protected static boolean udfsContains(String name) {
        if (name == null) {
            return false;
        }
        name = name.trim().toLowerCase();
        return udfSet.containsKey(name);
    }

    /**
     * 解析builder，主要用于union 场景
     *
     * @param sqlNode
     * @return
     */
    public static AbstractSQLBuilder parseBuilder(Object sqlNode) {
        ISqlParser parser = getParse(sqlNode);
        if (!(parser instanceof IBuilderCreator)) {
            throw new RuntimeException("sql node is can not parser to Descriptor " + sqlNode.toString());
        }
        IBuilderCreator creator = (IBuilderCreator) parser;
        AbstractSQLBuilder builder = creator.create();
        parser.parse(builder, sqlNode);
        return builder;
    }

    public static void addUDF(String functionName, FunctionSQLBuilder sqlBuilder) {
        if (functionName == null) {
            return;
        }
        udfSet.put(functionName.trim().toLowerCase(), sqlBuilder);
    }
}
