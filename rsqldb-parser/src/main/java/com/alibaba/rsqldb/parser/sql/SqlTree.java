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

import com.alibaba.rsqldb.parser.parser.SqlBuilderResult;
import com.alibaba.rsqldb.parser.sql.context.RootCreateSqlBuilderForSqlTreeContext;
import com.alibaba.rsqldb.parser.parser.builder.AbstractSqlBuilder;
import com.alibaba.rsqldb.parser.parser.builder.CreateSqlBuilder;
import com.alibaba.rsqldb.parser.parser.builder.FunctionSqlBuilder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.common.topology.ChainStage;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.common.topology.metric.StageGroup;
import org.apache.rocketmq.streams.common.topology.model.AbstractStage;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.common.utils.PrintUtil;
import org.apache.rocketmq.streams.common.utils.SQLFormatterUtil;

public class SqlTree {

    protected String namespace;

    /**
     * sql树的根节点对应的builder
     */
    protected AbstractSqlBuilder<?> sqlBuilder;

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
     * 优化后的sql
     */
    protected String sql;
    /**
     * 最顶层的create的tablename
     */
    protected String rootTableName;

    /**
     * protected AbstractSource abstractSource;//最源头的数据源，主要提供输出文件目录和输出字段脚本
     */
    public SqlTree(String pipelineName, AbstractSqlBuilder<?> sqlBuilder, List<FunctionSqlBuilder> functionBuilders) {
        this.namespace = sqlBuilder.getNamespace();
        this.sqlBuilder = sqlBuilder;
        if (sqlBuilder instanceof CreateSqlBuilder) {
            CreateSqlBuilder createSqlBuilder = (CreateSqlBuilder) sqlBuilder;
            rootTableName = createSqlBuilder.getTableName();
        }
        this.functionBuilders = functionBuilders;
        this.pipelineName = pipelineName;
    }

    public static SQLFormatterUtil sqlFormatterUtil = new SQLFormatterUtil();

    public PipelineBuilder build() {
        PipelineBuilder pipelineBuilder = new PipelineBuilder(namespace, pipelineName);
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
            RootCreateSqlBuilderForSqlTreeContext.getInstance().set((CreateSqlBuilder) sqlBuilder);
        }

        //TODO 拓扑结构在此
        Map<String, List<String>> tree = new HashMap<>();
        Map<AbstractSqlBuilder<?>, BuilderNodeStage> builderBuilderNodeStageMap = new HashMap<>();
        build(namespace, sqlBuilder, pipelineBuilder, tree, builderBuilderNodeStageMap);
        pipelineBuilder.getPipeline().setMsgSourceName(sqlBuilder.getTableName());
        pipelineBuilder.getPipeline().setCreateTableSQL(sqlFormatterUtil.format(sqlBuilder.getSqlNode().toString()));
        StringBuilder stringBuilder = new StringBuilder();
//        printTree(sqlBuilder.getTableName(), tree, stringBuilder);

        this.rootPipelineBuilder = pipelineBuilder;
        return this.rootPipelineBuilder;
    }

    /**
     * 建立一个拓扑结构。核心逻辑是一层一层的创建stage，通过next label建立拓扑关系
     *
     * @param namespace
     * @param sqlBuilder
     * @param currentBuilder
     */
    protected void build(String namespace, AbstractSqlBuilder<?> sqlBuilder, final PipelineBuilder currentBuilder,
        Map<String, List<String>> tree, Map<AbstractSqlBuilder<?>, BuilderNodeStage> nodeStageMap) {
        List<AbstractSqlBuilder<?>> list = sqlBuilder.getChildren();

        if (CollectionUtil.isEmpty(list)) {
            return;
        }
        List<String> subTables = new ArrayList<>();
        for (AbstractSqlBuilder<?> tmp : list) {
            subTables.add(tmp.getTableName());
        }
        tree.put(sqlBuilder.getTableName(), subTables);
        //一层对应的所有的分支。会从上层接收数据分成多份给每一个分支
        List<ChainStage<?>> chainStages = new ArrayList<>();
        //每个分支最后的输出节点
        List<ChainStage<?>> nextStages = new ArrayList<>();
        //有产生阶段的build，则会进入下一次构建
        List<AbstractSqlBuilder<?>> nextBuilders = new ArrayList<>();
        //对此节点所有child进行编译，并把每个分支最后一个节点形成列表，做递归
        for (AbstractSqlBuilder<?> builder : list) {

            BuilderNodeStage nodeStage = nodeStageMap.get(builder);
            ChainStage<?> first = null;
            ChainStage<?> last = null;
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
        for (ChainStage<?> stage : nextStages) {
            currentBuilder.setCurrentChainStage(stage);
            AbstractSqlBuilder nextBuilder = nextBuilders.get(i);
            build(namespace, nextBuilder, currentBuilder, tree, nodeStageMap);
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

        PipelineBuilder pipelineBuilder = new PipelineBuilder(namespace, pipelineName);
        pipelineBuilder.setParentTableName(parentName);
        pipelineBuilder.setRootTableName(rootTableName);
        builder.setPipelineBuilder(pipelineBuilder);
        //如果是双流join，且是join中的右流join

        SqlBuilderResult sqlBuilderResult = builder.buildSql();

        return sqlBuilderResult;
    }

    public PipelineBuilder getRootPipelineBuilder() {
        return rootPipelineBuilder;
    }

    public AbstractSqlBuilder getRootBuilder() {
        return this.sqlBuilder;
    }

    /**
     * 获取叶子节点
     *
     * @param chainStage
     * @param stageMap
     * @param list
     */
    protected void getLastChainStage(ChainStage chainStage, Map<String, AbstractStage> stageMap,
        List<ChainStage> list) {
        if (chainStage.getNextStageLabels() == null || chainStage.getNextStageLabels().size() == 0) {
            list.add(chainStage);
            return;
        }
        List<String> nextStageLabels = chainStage.getNextStageLabels();
        for (String nextLabel : nextStageLabels) {
            ChainStage stage = (ChainStage) stageMap.get(nextLabel);
            getLastChainStage(stage, stageMap, list);
        }
    }

    public ChainPipeline buildPipeline(IConfigurableService configurableService) {
        return this.rootPipelineBuilder.build(configurableService);
    }

    protected void printTree(String rootName, Map<String, List<String>> tree, StringBuilder sb) {
        sb.append(rootName);
        List<String> children = tree.get(rootName);
        if (children == null) {
            return;
        }
        for (String item : children) {
            sb.append("->");
            printTree(item, tree, sb);
            if (tree.get(item) == null) {
                sb.append(PrintUtil.LINE);
            }
            sb.append(rootName + "->");
        }

    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    protected class BuilderNodeStage {
        ChainStage firstStage;
        ChainStage lastStage;

        public BuilderNodeStage(ChainStage firstStage, ChainStage lastStage) {
            this.firstStage = firstStage;
            this.lastStage = lastStage;
        }
    }

    public String getRootTableName() {
        return rootTableName;
    }

    public void setRootTableName(String rootTableName) {
        this.rootTableName = rootTableName;
    }
}
