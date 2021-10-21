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
package com.alibaba.rsqldb.parser.parser.builder;

import java.util.HashSet;
import java.util.Set;

public class ViewSQLBuilder extends AbstractSQLBuilder<AbstractSQLBuilder> {

    protected ISQLBuilder builder;

    protected Set<String> fieldNames = new HashSet<>();

    @Override
    public void build() {
        if (builder != null) {
            builder.setPipelineBuilder(pipelineBuilder);
            if (AbstractSQLBuilder.class.isInstance(builder)) {
                AbstractSQLBuilder abstractSQLBuilder = (AbstractSQLBuilder)builder;
                // abstractSQLBuilder.setTreeSQLBulider(getTreeSQLBulider());
                abstractSQLBuilder.setTableName2Builders(getTableName2Builders());
            }
            AbstractSQLBuilder abstractSQLBuilder = (AbstractSQLBuilder)builder;
            abstractSQLBuilder.addRootTableName(this.getRootTableNames());
            abstractSQLBuilder.buildSQL();
        }

    }

    @Override
    public String getFieldName(String fieldName, boolean containsSelf) {
        if (builder != null && AbstractSQLBuilder.class.isInstance(builder)) {
            AbstractSQLBuilder abstractSQLBuilder = (AbstractSQLBuilder)builder;
            return abstractSQLBuilder.getFieldName(fieldName, containsSelf);
        }
        return null;
    }

    @Override
    public Set<String> parseDependentTables() {
        if (builder != null) {
            if (AbstractSQLBuilder.class.isInstance(builder)) {
                AbstractSQLBuilder abstractSQLBuilder = (AbstractSQLBuilder)builder;
                // abstractSQLBuilder.setTreeSQLBulider(getTreeSQLBulider());
            }
            return builder.parseDependentTables();
        }
        return new HashSet<>();
    }

    public Set<String> getFieldNames() {
        return fieldNames;
    }

    public void setFieldNames(Set<String> fieldNames) {
        this.fieldNames = fieldNames;
    }

    public ISQLBuilder getBuilder() {
        return builder;
    }

    public void setBuilder(ISQLBuilder builder) {
        this.builder = builder;
    }
}
