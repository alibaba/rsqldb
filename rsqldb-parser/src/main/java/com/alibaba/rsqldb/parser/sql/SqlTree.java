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
package com.alibaba.rsqldb.parser.sql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.alibaba.rsqldb.parser.SqlBuilderResult;
import com.alibaba.rsqldb.parser.builder.AbstractSqlBuilder;
import com.alibaba.rsqldb.parser.builder.CreateSqlBuilder;
import com.alibaba.rsqldb.parser.builder.FunctionSqlBuilder;
import com.alibaba.rsqldb.parser.sql.context.RootCreateSqlBuilderForSqlTreeContext;

import com.google.common.collect.Maps;
import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.common.model.SQLCompileContextForSource;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.common.topology.metric.StageGroup;
import org.apache.rocketmq.streams.common.topology.model.AbstractChainStage;
import org.apache.rocketmq.streams.common.topology.model.ChainPipeline;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.common.utils.SQLFormatterUtil;

public class SqlTree {

    public static SQLFormatterUtil sqlFormatterUtil = new SQLFormatterUtil();
    protected String namespace;
    /**
     * sql树的根节点对应的builder
     */
    protected AbstractSqlBuilder sqlBuilder;
    /**
     * 所有udf声明的builder
     */
    protected List<FunctionSqlBuilder> functionBuilders;
    protected String pipelineName;
    /**
     * sql树的根节点对应的creator，用于生成pipeline
     */
    protected PipelineBuilder rootPipelineBuilder;
    /**
     * 最顶层的create的table name
     */
    protected String rootTableName;
    protected Properties jobConfiguration;

    /**
     * protected AbstractSource abstractSource;//最源头的数据源，主要提供输出文件目录和输出字段脚本
     */
    public SqlTree(String pipelineName, AbstractSqlBuilder sqlBuilder, List<FunctionSqlBuilder> functionBuilders, Properties jobConfiguration) {
        this.namespace = sqlBuilder.getNamespace();
        this.sqlBuilder = sqlBuilder;
        if (sqlBuilder instanceof CreateSqlBuilder) {
            CreateSqlBuilder createSqlBuilder = (CreateSqlBuilder)sqlBuilder;
            rootTableName = createSqlBuilder.getTableName();
        }
        this.functionBuilders = functionBuilders;
        this.pipelineName = pipelineName;
        this.jobConfiguration = jobConfiguration;
    }

    public PipelineBuilder build() {
        PipelineBuilder pipelineBuilder = new PipelineBuilder(namespace, pipelineName, jobConfiguration);
        if (functionBuilders != null) {
            for (FunctionSqlBuilder functionSqlBuilder : functionBuilders) {
                functionSqlBuilder.setPipelineBuilder(pipelineBuilder);
                functionSqlBuilder.build();
            }
        }
        pipelineBuilder.setRootTableName(sqlBuilder.getTableName());
        sqlBuilder.setPipelineBuilder(pipelineBuilder);
        SqlBuilderResult sqlBuilderResult = sqlBuilder.buildSql();
        if (sqlBuilderResult.getStages().size() > 0) {
            addStageGroupPipeline(sqlBuilderResult.getStageGroup(), pipelineBuilder.getPipeline());
        }

        if (sqlBuilder instanceof CreateSqlBuilder) {
            RootCreateSqlBuilderForSqlTreeContext.getInstance().set((CreateSqlBuilder)sqlBuilder);
            SQLCompileContextForSource.getInstance().set(((CreateSqlBuilder)sqlBuilder).getSource());
        }

        // 拓扑结构在此
        Map<String, List<String>> tree = new HashMap<>();
        build(sqlBuilder, pipelineBuilder, tree, Maps.newHashMap());
        pipelineBuilder.getPipeline().setMsgSourceName(sqlBuilder.getTableName());
        pipelineBuilder.getPipeline().setCreateTableSQL(sqlFormatterUtil.format(sqlBuilder.getSqlNode().toString()));
        pipelineBuilder.setConfiguration(sqlBuilder.getConfiguration());

        this.rootPipelineBuilder = pipelineBuilder;
        return this.rootPipelineBuilder;
    }

    /**
     * 建立一个拓扑结构。核心逻辑是一层一层的创建stage，通过next label建立拓扑关系
     *
     * @param sqlBuilder
     * @param currentBuilder
     */
    protected void build(AbstractSqlBuilder sqlBuilder, final PipelineBuilder currentBuilder, Map<String, List<String>> tree, Map<AbstractSqlBuilder, BuilderNodeStage> nodeStageMap) {
        List<AbstractSqlBuilder> list = sqlBuilder.getChildren();

        if (CollectionUtil.isEmpty(list)) {
            return;
        }
        List<String> subTables = new ArrayList<>();
        for (AbstractSqlBuilder tmp : list) {
            subTables.add(tmp.getTableName());
        }
        tree.put(sqlBuilder.getTableName(), subTables);
        //一层对应的所有的分支。会从上层接收数据分成多份给每一个分支
        List<AbstractChainStage<?>> chainStages = new ArrayList<>();
        //每个分支最后的输出节点
        List<AbstractChainStage<?>> nextStages = new ArrayList<>();
        //有产生阶段的build，则会进入下一次构建
        List<AbstractSqlBuilder> nextBuilders = new ArrayList<>();
        //对此节点所有child进行编译，并把每个分支最后一个节点形成列表，做递归
        for (AbstractSqlBuilder builder : list) {

            BuilderNodeStage nodeStage = nodeStageMap.get(builder);
            AbstractChainStage<?> first = null;
            AbstractChainStage<?> last = null;
            boolean isRightJoin = false;
            SqlBuilderResult sqlBuilderResult = null;
            if (nodeStage == null) {
                sqlBuilderResult = builderPipeline(builder, sqlBuilder.getTableName(), this.sqlBuilder.getTableName());
                isRightJoin = sqlBuilderResult.isRightJoin();
                List<IConfigurable> configurableList = sqlBuilderResult.getConfigurables();
                if (CollectionUtil.isNotEmpty(configurableList)) {
                    currentBuilder.addConfigurables(configurableList);
                }
                if (CollectionUtil.isNotEmpty(sqlBuilderResult.getStages())) {
                    currentBuilder.getPipeline().getStages().addAll(sqlBuilderResult.getStages());
                }
                if (sqlBuilderResult.getStages() != null && sqlBuilderResult.getStages().size() > 0) {
                    first = sqlBuilderResult.getFirstStage();
                    last = sqlBuilderResult.getLastStage();
                } else {
                    last = currentBuilder.getCurrentChainStage();
                }

            } else {
                first = nodeStage.firstStage;
                last = nodeStage.lastStage;
            }
            if (sqlBuilderResult != null) {
                addStageGroupPipeline(sqlBuilderResult.getStageGroup(), currentBuilder.getPipeline());
            }

            last.setOwnerSqlNodeTableName(builder.getTableName());
            last.setMsgSourceName(builder.getTableName());
            //右流不需要再挂字节点，因为默认join 后续节点都走左流
            if (!isRightJoin) {
                nextStages.add(last);
                nextBuilders.add(builder);
            }

            if (first != null) {
                chainStages.add(first);
            }
            nodeStageMap.put(builder, new BuilderNodeStage(first, last));
        }

        if (chainStages.size() > 0) {
            currentBuilder.setHorizontalStages(chainStages);
        }

        if (nextStages.size() == 0) {
            return;
        }
        int i = 0;
        for (AbstractChainStage<?> stage : nextStages) {
            currentBuilder.setCurrentChainStage(stage);
            AbstractSqlBuilder nextBuilder = nextBuilders.get(i);
            build(nextBuilder, currentBuilder, tree, nodeStageMap);
            i++;
        }
    }

    private void addStageGroupPipeline(StageGroup group, ChainPipeline<?> pipeline) {
        pipeline.addStageGroup(group);
        List<StageGroup> stageGroups = group.getChildren();
        if (stageGroups != null) {
            for (StageGroup stageGroup : stageGroups) {
                addStageGroupPipeline(stageGroup, pipeline);
            }
        }
    }

    /**
     * build pipeline，如果是双流join的右分支，特殊处理，否则正常创建
     *
     * @param builder
     * @return
     */
    protected SqlBuilderResult builderPipeline(AbstractSqlBuilder builder, String parentName, String rootTableName) {

        PipelineBuilder pipelineBuilder = new PipelineBuilder(namespace, pipelineName, jobConfiguration);
        pipelineBuilder.setParentTableName(parentName);
        pipelineBuilder.setRootTableName(rootTableName);
        builder.setPipelineBuilder(pipelineBuilder);
        //如果是双流join，且是join中的右流join

        return builder.buildSql();
    }

    public PipelineBuilder getRootPipelineBuilder() {
        return rootPipelineBuilder;
    }

    protected static class BuilderNodeStage {
        AbstractChainStage<?> firstStage;
        AbstractChainStage<?> lastStage;

        public BuilderNodeStage(AbstractChainStage<?> firstStage, AbstractChainStage<?> lastStage) {
            this.firstStage = firstStage;
            this.lastStage = lastStage;
        }
    }
}
