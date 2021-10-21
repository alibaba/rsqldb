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
package com.alibaba.rsqldb.parser.parser.result;

import com.alibaba.rsqldb.parser.parser.builder.AbstractSQLBuilder;

/**
 * 每个解析的返回结果。需要考虑变量，常量和脚本的情况
 */
public class BuilderParseResult implements IParseResult<String> {

    /**
     * 保存3种值，常量，变量的值以及脚本如果有需要返回的变量，也保存在这个字段
     */
    protected AbstractSQLBuilder builder;

    /**
     * 注意这里的脚本
     *
     * @param value
     */
    public BuilderParseResult(AbstractSQLBuilder value) {
        this.builder = value;
    }

    /**
     * 如果获取的值是为了做子表达式子，会去掉最后的分号
     *
     * @return
     */
    @Override
    public String getValueForSubExpression() {
        return builder.getTableName();
    }

    @Override
    public String getReturnValue() {
        return builder.getTableName();
    }

    @Override
    public String getResultValue() {
        return builder.getSqlNode().toString();
    }

    @Override
    public boolean isConstantString() {
        return false;
    }

    public AbstractSQLBuilder getBuilder() {
        return builder;
    }

}
