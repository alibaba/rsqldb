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

import com.alibaba.rsqldb.parser.parser.SqlBuilderResult;
import com.alibaba.rsqldb.parser.sql.context.FieldsOfTableForSqlTreeContext;
import java.util.HashSet;
import java.util.Set;
import org.apache.rocketmq.streams.common.model.NameCreatorContext;
import org.apache.rocketmq.streams.common.topology.ChainStage;
import org.apache.rocketmq.streams.common.topology.builder.IStageBuilder;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.common.topology.stages.EmptyChainStage;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;

public class TableNodeBuilder extends SelectSqlBuilder {

    @Override
    protected void build() {

    }

    @Override public SqlBuilderResult buildSql() {
        ChainStage first= CollectionUtil.isEmpty(this.pipelineBuilder.getFirstStages()) ?null:this.pipelineBuilder.getFirstStages().get(0);
        if(first==null){
            first=new EmptyChainStage();
            first.setLabel(NameCreatorContext.get().createName("empty"));
            final ChainStage chainStage=first;
            this.pipelineBuilder.addChainStage(new IStageBuilder<ChainStage>() {
                @Override public ChainStage createStageChain(PipelineBuilder pipelineBuilder) {
                    return chainStage;
                }

                @Override public void addConfigurables(PipelineBuilder pipelineBuilder) {

                }
            });
        }
        SqlBuilderResult sqlBuilderResult= new SqlBuilderResult(this.pipelineBuilder,first,first);
        sqlBuilderResult.getStageGroup().setSql("SELECT * FROM "+getTableName());
        sqlBuilderResult.getStageGroup().setViewName("FROM "+getTableName());
        return sqlBuilderResult;
    }

    @Override
    public Set<String> parseDependentTables() {
        Set<String> tables = new HashSet<>();
        tables.add(getTableName());
        return tables;
    }

    @Override
    public Set<String> getAllFieldNames() {

        return FieldsOfTableForSqlTreeContext.getInstance().get().get(getTableName());
    }

    @Override
    public String getFieldName(String fieldName) {
        String name = doAllFieldName(fieldName);
        if (name != null) {
            return name;
        }
        String tableName = getTableName();
        Set<String> fieldNames = FieldsOfTableForSqlTreeContext.getInstance().get().get(tableName);
        if(fieldNames.contains(fieldName)){
            return fieldName;
        }
        String asName = getAsName();
        int index = fieldName.indexOf(".");
        if (index == -1) {
            //if(fieldNames==null){
            //    System.out.println("");
            //}
            if (fieldNames == null) {
                return fieldName;
            }
            if (fieldNames.contains(fieldName)) {
                return fieldName;
            } else {
                return null;
            }
        }
        String ailasName = fieldName.substring(0, index);
        fieldName = fieldName.substring(index + 1);
        if (ailasName.equals(asName) && fieldNames.contains(fieldName)) {
            return fieldName;
        } else {
            return null;
        }
    }

    @Override
    public String getFieldName(String fieldName, boolean containsSelf) {
        return getFieldName(fieldName);
    }

}