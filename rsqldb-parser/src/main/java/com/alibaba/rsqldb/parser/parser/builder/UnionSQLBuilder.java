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

import org.apache.rocketmq.streams.common.model.NameCreator;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.common.topology.ChainStage;
import org.apache.rocketmq.streams.common.topology.builder.IStageBuilder;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.common.topology.stages.UnionChainStage;
import org.apache.rocketmq.streams.common.utils.NameCreatorUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class UnionSQLBuilder extends SelectSQLBuilder {

    private static final Log LOG = LogFactory.getLog(UnionSQLBuilder.class);

    protected List<AbstractSQLBuilder> builders = new ArrayList<>();
    protected Set<String> tableNames;

    @Override
    public void buildSQL() {
        if (builders == null) {
            return;
        }
        String namespace = getPipelineBuilder().getPipelineNameSpace();
        String name = getPipelineBuilder().getPipelineName();
        PipelineBuilder rootBuilder = getPipelineBuilder();
        UnionChainStage unionChainStage = new UnionChainStage();
        String lableName = NameCreatorUtil.createNewName("subpipline", name, "union");
        unionChainStage.setLabel(lableName);
        rootBuilder.addChainStage(new IStageBuilder<ChainStage>() {

            @Override
            public ChainStage createStageChain(PipelineBuilder pipelineBuilder) {
                return unionChainStage;
            }

            @Override
            public void addConfigurables(PipelineBuilder pipelineBuilder) {

            }
        });
        Map<String, String> piplineName2BuilderTableNames = new HashMap<>();//这个pipline的上层输入stage对应的tablename
        for (AbstractSQLBuilder builder : builders) {
            String piplineName = NameCreator.createNewName("subpipline", name, "union");
            PipelineBuilder pipelineBuilder = new PipelineBuilder(namespace, piplineName);
            builder.setPipelineBuilder(pipelineBuilder);
            //builder.setTreeSQLBulider(getTreeSQLBulider());
            builder.buildSQL();
            ChainPipeline chainPipline = pipelineBuilder.getPipeline();
            unionChainStage.addPipline(chainPipline);
            rootBuilder.addConfigurables(pipelineBuilder.getConfigurables());
            piplineName2BuilderTableNames.put(chainPipline.getConfigureName(), builder.getTableName());

        }
        unionChainStage.setPiplineName2MsgSourceName(piplineName2BuilderTableNames);

    }
    public boolean containsTableName(String tableName){
      return   tableNames.contains(tableName);
    }

    @Override
    public Set<String> parseDependentTables() {
        if (builders == null) {
            return new HashSet<>();
        }
        Set<String> dependentTableNames = new HashSet<>();
        for (AbstractSQLBuilder descriptor : builders) {
            //builder.setTreeSQLBulider(getTreeSQLBulider());
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
        for (AbstractSQLBuilder builder : builders) {
            if (SelectSQLBuilder.class.isInstance(builder)) {
                SelectSQLBuilder selectSQLBuilder = (SelectSQLBuilder)builder;
                return selectSQLBuilder.getAllFieldNames();
            }
        }
        return null;
    }

    @Override
    public String getFieldName(String fieldName) {
        for (AbstractSQLBuilder builder : builders) {
            if (SelectSQLBuilder.class.isInstance(builder)) {
                SelectSQLBuilder selectSQLBuilder = (SelectSQLBuilder)builder;
                Object value = selectSQLBuilder.getFieldName(fieldName, true);
                if (value != null) {
                    String tmp = (String)value;
                    if (tmp.indexOf(".") != -1) {
                        return tmp.substring(tmp.indexOf("."));
                    } else {
                        return tmp;
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

    public void addBuilder(AbstractSQLBuilder builder) {
        builders.add(builder);
    }

    public List<AbstractSQLBuilder> getBuilders() {
        return builders;
    }
}
