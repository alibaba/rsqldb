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
package com.alibaba.rsqldb.parser.builder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alibaba.rsqldb.parser.SqlBuilderResult;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.model.NameCreatorContext;
import org.apache.rocketmq.streams.common.topology.IStageBuilder;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.common.topology.metric.StageGroup;
import org.apache.rocketmq.streams.common.topology.model.AbstractChainStage;
import org.apache.rocketmq.streams.common.topology.model.AbstractStage;
import org.apache.rocketmq.streams.common.topology.stages.UnionEndChainStage;
import org.apache.rocketmq.streams.common.topology.stages.UnionStartChainStage;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;

public class UnionSqlBuilder extends SelectSqlBuilder {

    private static final Log LOG = LogFactory.getLog(UnionSqlBuilder.class);

    protected List<AbstractSqlBuilder> builders = new ArrayList<>();
    protected Set<String> tableNames;
    protected boolean hasCompile = false;
    protected Map<String, Set<String>> msgSource2LableNames = new HashMap<>();

    @Override
    public SqlBuilderResult buildSql() {
        if (builders == null) {
            return null;
        }
        String namespace = getPipelineBuilder().getPipelineNameSpace();
        String name = getPipelineBuilder().getPipelineName();
        PipelineBuilder rootBuilder = getPipelineBuilder();
        UnionStartChainStage start = new UnionStartChainStage();
        String labelName = NameCreatorContext.get().createName("union", name, "start");
        start.setLabel(labelName);
        UnionEndChainStage end = new UnionEndChainStage();
        labelName = NameCreatorContext.get().createName("union", name, "end");
        end.setLabel(labelName);
        UnionStartChainStage startStage = (UnionStartChainStage)rootBuilder.addChainStage(new IStageBuilder<>() {

            @Override
            public AbstractChainStage<?> createStageChain(PipelineBuilder pipelineBuilder) {
                return start;
            }

            @Override
            public void addConfigurables(PipelineBuilder pipelineBuilder) {

            }
        });
        AbstractChainStage endStage = rootBuilder.addChainStage(new IStageBuilder<>() {

            @Override
            public AbstractChainStage<?> createStageChain(PipelineBuilder pipelineBuilder) {
                return end;
            }

            @Override
            public void addConfigurables(PipelineBuilder pipelineBuilder) {

            }
        });
        rootBuilder.setHorizontalStages(startStage);
        rootBuilder.setCurrentChainStage(startStage);
        List<AbstractStage<?>> stages = new ArrayList<>();
        stages.add(startStage);
        stages.add(endStage);
        rootBuilder.setCurrentStageGroup(new StageGroup(startStage, endStage, stages));
        rootBuilder.setParentStageGroup(rootBuilder.getCurrentStageGroup());

        //这个pipeline的上层输入stage对应的tablename
        Map<String, Set<String>> msgSource2LableNames = new HashMap<>();
        for (AbstractSqlBuilder builder : builders) {
            PipelineBuilder pipelineBuilder = createPipelineBuilder();
            builder.setPipelineBuilder(pipelineBuilder);
            builder.setConfiguration(getConfiguration());
            SqlBuilderResult sqlBuilderResult = builder.buildSql();
            for (String msgSource : builder.parseDependentTables()) {
                Set<String> lables = msgSource2LableNames.get(msgSource);
                if (lables == null) {
                    lables = new HashSet<>();
                    msgSource2LableNames.put(msgSource, lables);
                }
                lables.add(sqlBuilderResult.getFirstStage().getLabel());
            }
            mergeSQLBuilderResult(sqlBuilderResult);
            rootBuilder.setHorizontalStages(endStage);
            rootBuilder.setCurrentChainStage(startStage);
        }
        rootBuilder.setCurrentChainStage(endStage);
        startStage.setMsgSource2StageLables(msgSource2LableNames);
        SqlBuilderResult sqlBuilderResult = new SqlBuilderResult(pipelineBuilder, startStage, endStage);
        if (sqlBuilderResult.getStageGroup().getSql() == null) {
            sqlBuilderResult.getStageGroup().setViewName(createSQLFromParser());
            sqlBuilderResult.getStageGroup().setSql(sqlFormatterUtil.format(createSQLFromParser()));
        }
        return sqlBuilderResult;
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
        for (AbstractSqlBuilder descriptor : builders) {
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
        for (AbstractSqlBuilder builder : builders) {
            if (builder instanceof SelectSqlBuilder) {
                SelectSqlBuilder selectSqlBuilder = (SelectSqlBuilder)builder;
                return selectSqlBuilder.getAllFieldNames();
            }
        }
        return null;
    }

    @Override
    public String getFieldName(String fieldName) {
        for (AbstractSqlBuilder builder : builders) {
            if (builder instanceof SelectSqlBuilder) {
                SelectSqlBuilder selectSqlBuilder = (SelectSqlBuilder)builder;
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

    @Override
    public String createSQLFromParser() {
        StringBuilder sb = new StringBuilder();
        sb.append("UNION(");
        sb.append(this.builders.size() + ",");
        sb.append(MapKeyUtil.createKeyFromCollection(",", this.getTableNames()));
        sb.append(")");
        return sb.toString();
    }

    public Set<String> getTableNames() {
        return tableNames;
    }

    public void setTableNames(Set<String> tableNames) {
        this.tableNames = tableNames;
    }

    public void addBuilder(AbstractSqlBuilder builder) {
        builders.add(builder);
    }

    public List<AbstractSqlBuilder> getBuilders() {
        return builders;
    }
}