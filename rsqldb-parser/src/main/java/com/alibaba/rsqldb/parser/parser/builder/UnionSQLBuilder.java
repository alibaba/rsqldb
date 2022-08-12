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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.model.NameCreator;
import org.apache.rocketmq.streams.common.model.NameCreatorContext;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.common.topology.ChainStage;
import org.apache.rocketmq.streams.common.topology.builder.IStageBuilder;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.common.topology.stages.UnionChainStage;
import org.apache.rocketmq.streams.common.utils.StringUtil;

public class UnionSQLBuilder extends SelectSQLBuilder {

    private static final Log LOG = LogFactory.getLog(UnionSQLBuilder.class);

    protected List<AbstractSQLBuilder<?>> builders = new ArrayList<>();
    protected Set<String> tableNames;
    protected boolean hasCompile=false;
    protected Map<String, Set<String>> pipelineName2BuilderTableNames;
    protected List<String> pipelineNames;
    @Override
    public void buildSql() {
        if (builders == null) {
            return;
        }
        String namespace = getPipelineBuilder().getPipelineNameSpace();
        String name = getPipelineBuilder().getPipelineName();
        PipelineBuilder rootBuilder = getPipelineBuilder();
        UnionChainStage<?> unionChainStage = new UnionChainStage<>();
        String labelName = NameCreatorContext.get().createNewName(name, "subpipline", "union");
        unionChainStage.setLabel(labelName);
        rootBuilder.addChainStage(new IStageBuilder<ChainStage>() {

            @Override
            public ChainStage<?> createStageChain(PipelineBuilder pipelineBuilder) {
                return unionChainStage;
            }

            @Override
            public void addConfigurables(PipelineBuilder pipelineBuilder) {

            }
        });
        if(hasCompile){
            unionChainStage.setPiplineNames(this.pipelineNames);
            unionChainStage.setPiplineName2MsgSourceName(this.pipelineName2BuilderTableNames);
            return;
        }
        //这个pipeline的上层输入stage对应的tablename
        Map<String, Set<String>> pipelineName2BuilderTableNames = new HashMap<>();
        for (AbstractSQLBuilder<?> builder : builders) {
            String pipelineName = NameCreatorContext.get().createNewName( name, "subpipline", "union");
            PipelineBuilder pipelineBuilder = new PipelineBuilder(namespace, pipelineName);
            builder.setPipelineBuilder(pipelineBuilder);
            builder.buildSql();
            ChainPipeline<?> chainPipeline = pipelineBuilder.getPipeline();
            unionChainStage.addPipline(chainPipeline);
            rootBuilder.addConfigurables(pipelineBuilder.getConfigurables());
            pipelineName2BuilderTableNames.put(chainPipeline.getConfigureName(), builder.parseDependentTables());

        }
        unionChainStage.setPiplineName2MsgSourceName(pipelineName2BuilderTableNames);
        hasCompile=true;
        this.pipelineNames=unionChainStage.getPiplineNames();
        this.pipelineName2BuilderTableNames=pipelineName2BuilderTableNames;
    }

    public boolean containsTableName(String tableName) {
        return tableNames.contains(tableName);
    }

    @Override
    public Set<String> parseDependentTables() {
        if (builders == null) {
            return new HashSet<>();
        }
        Set<String> dependentTableNames = new HashSet<>();
        for (AbstractSQLBuilder<?> descriptor : builders) {
            Set<String> tables = descriptor.parseDependentTables();
            if (tables != null && tables.size() > 0) {
                for (String tableName : tables) {
                    if (StringUtil.isNotEmpty(tableName)) {
                        dependentTableNames.add(tableName);
                    }
                }

            }

        }
        return dependentTableNames;
    }

    @Override
    public Set<String> getAllFieldNames() {
        for (AbstractSQLBuilder<?> builder : builders) {
            if (builder instanceof SelectSQLBuilder) {
                SelectSQLBuilder selectSqlBuilder = (SelectSQLBuilder) builder;
                return selectSqlBuilder.getAllFieldNames();
            }
        }
        return null;
    }

    @Override
    public String getFieldName(String fieldName) {
        for (AbstractSQLBuilder<?> builder : builders) {
            if (builder instanceof SelectSQLBuilder) {
                SelectSQLBuilder selectSqlBuilder = (SelectSQLBuilder) builder;
                String value = selectSqlBuilder.getFieldName(fieldName, true);
                if (value != null) {
                    if (value.contains(".")) {
                        return value.substring(value.indexOf("."));
                    } else {
                        return value;
                    }
                }
            }
        }
        return null;
    }

    public Set<String> getTableNames() {
        return tableNames;
    }

    public void setTableNames(Set<String> tableNames) {
        this.tableNames = tableNames;
    }

    public void addBuilder(AbstractSQLBuilder<?> builder) {
        builders.add(builder);
    }

    public List<AbstractSQLBuilder<?>> getBuilders() {
        return builders;
    }
}
