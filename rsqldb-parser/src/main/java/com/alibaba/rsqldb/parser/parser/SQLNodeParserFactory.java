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

import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.rocketmq.streams.common.model.NameCreator;
import org.apache.rocketmq.streams.common.calssscaner.AbstractScan;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import com.alibaba.rsqldb.parser.parser.builder.AbstractSQLBuilder;
import com.alibaba.rsqldb.parser.parser.builder.SelectSQLBuilder;
import com.alibaba.rsqldb.parser.parser.result.IParseResult;
import com.alibaba.rsqldb.parser.parser.result.ScriptParseResult;
import com.alibaba.rsqldb.parser.parser.sqlnode.IBuilderCreator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;

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
    private static Set<String> udfSet = new HashSet<>();

    private static AbstractScan scan = new AbstractScan() {
        @Override
        protected void doProcessor(Class clazz) {
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
        Iterator<ISqlParser> it = sqlParsers.values().iterator();
        while (it.hasNext()) {
            ISqlParser sqlParser = it.next();
            if (sqlParser.support(sqlNode)) {
                return sqlParser;
            }
        }
        if (SqlBasicCall.class.isInstance(sqlNode)) {
            final SqlBasicCall sqlBasicCall = (SqlBasicCall)sqlNode;
            String name = sqlBasicCall.getOperator().getName().toUpperCase();
            if (udfsContains(name)) {
                return new ISqlParser() {

                    @Override
                    public IParseResult parse(AbstractSQLBuilder builder, Object o) {
                        if (!SelectSQLBuilder.class.isInstance(builder)) {
                            throw new RuntimeException(
                                "can not support parser udf " + name + ", expect select sql builder ,real is " + builder
                                    .getClass().getName());
                        }
                        String returnValue = NameCreator.createNewName("__", name);
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
                                scriptValue = scriptValue + getParse(node).parse(builder, node)
                                    .getValueForSubExpression();
                            }
                        }
                        scriptValue = scriptValue + ");";
                        ScriptParseResult scriptParseResult = new ScriptParseResult();
                        scriptParseResult.addScript((SelectSQLBuilder)builder, scriptValue);
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
        throw new RuntimeException("can not support this parser " + sqlNode.toString());
    }

    protected static boolean udfsContains(String name) {
        if (name == null) {
            return false;
        }
        name = name.trim().toLowerCase();
        return udfSet.contains(name);
    }

    /**
     * 解析builder，主要用于union 场景
     *
     * @param sqlNode
     * @return
     */
    public static AbstractSQLBuilder parseBuilder(Object sqlNode) {
        ISqlParser parser = getParse(sqlNode);
        if (!IBuilderCreator.class.isInstance(parser)) {
            throw new RuntimeException("sql node is can not parser to Descriptor " + sqlNode.toString());
        }
        IBuilderCreator creator = (IBuilderCreator)parser;
        AbstractSQLBuilder builder = creator.create();
        parser.parse(builder, sqlNode);
        return builder;
    }

    public static void addUDF(String functionName) {
        if (functionName == null) {
            return;
        }
        udfSet.add(functionName.trim().toLowerCase());
    }
}
