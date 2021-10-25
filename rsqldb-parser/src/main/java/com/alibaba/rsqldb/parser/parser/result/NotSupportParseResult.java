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

import org.apache.calcite.sql.SqlNode;

/**
 * 每个解析的返回结果。需要考虑变量，常量和脚本的情况
 */
public class NotSupportParseResult implements IParseResult {

    protected SqlNode sqlNode;

    /**
     * 注意这里的脚本
     */
    public NotSupportParseResult(SqlNode sqlNode) {
        this.sqlNode = sqlNode;
    }

    public SqlNode getSqlNode() {
        return sqlNode;
    }

    /**
     * 如果获取的值是为了做子表达式子，会去掉最后的分号
     *
     * @return
     */
    @Override
    public String getValueForSubExpression() {
        return null;
    }

    @Override
    public String getReturnValue() {
        return null;
    }

    @Override
    public Object getResultValue() {
        return getDataValue();
    }

    @Override
    public boolean isConstantString() {
        return false;
    }

    protected <T> T getDataValue() {
        return null;
    }
}
