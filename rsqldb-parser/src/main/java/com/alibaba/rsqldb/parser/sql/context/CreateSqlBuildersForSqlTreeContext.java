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
package com.alibaba.rsqldb.parser.sql.context;

import java.util.List;
import java.util.Map;

import com.alibaba.rsqldb.parser.builder.CreateSqlBuilder;
import com.alibaba.rsqldb.parser.builder.ISqlNodeBuilder;

public class CreateSqlBuildersForSqlTreeContext extends ThreadLocal<Map<String, CreateSqlBuilder>> {
    private static final CreateSqlBuildersForSqlTreeContext INSTANCE = new CreateSqlBuildersForSqlTreeContext();
    protected List<ISqlNodeBuilder> createSqlBuilders;

    private CreateSqlBuildersForSqlTreeContext() {
    }

    public static void setParserCreateSqlBuilder(List<ISqlNodeBuilder> createSqlBuilders) {
        INSTANCE.setCreateSqlBuilders(createSqlBuilders);
    }

    public static CreateSqlBuildersForSqlTreeContext getInstance() {
        return INSTANCE;
    }

    public List<ISqlNodeBuilder> getCreateSqlBuilders() {
        return createSqlBuilders;
    }

    protected void setCreateSqlBuilders(List<ISqlNodeBuilder> createSqlBuilders) {
        this.createSqlBuilders = createSqlBuilders;
    }
}