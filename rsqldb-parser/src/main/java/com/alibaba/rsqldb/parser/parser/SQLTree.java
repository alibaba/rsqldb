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
package com.alibaba.rsqldb.parser.parser;

import com.alibaba.rsqldb.parser.parser.builder.AbstractSQLBuilder;
import com.alibaba.rsqldb.parser.parser.builder.CreateSQLBuilder;
import com.alibaba.rsqldb.parser.parser.builder.FunctionSQLBuilder;
import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.common.topology.ChainStage;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.common.topology.model.AbstractStage;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.common.utils.PrintUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SQLTree {

    protected String namespace;

    /**
     * sql树的根节点对应的builder
     */
    protected AbstractSQLBuilder sqlBuilder;

    /**
     * 所有udf声明的builder
     */
    protected List<FunctionSQLBuilder> functionBuilders;

    protected String pipelineName;

    /**
     * sql树的根节点对应的creator，用于生成pipeline
     */
    protected PipelineBuilder rootCreator;

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
     *
     * @param pipeline
     * @param sqlBuilder
     * @param functionBuilders
     */
    public SQLTree(String pipeline, AbstractSQLBuilder sqlBuilder, List<FunctionSQLBuilder> functionBuilders) {
        this.namespace = sqlBuilder.getNamespace();
        this.sqlBuilder = sqlBuilder;
        if (sqlBuilder instanceof CreateSQLBuilder) {
            CreateSQLBuilder createSqlBuilder = (CreateSQLBuilder) sqlBuilder;
            rootTableName = createSqlBuilder.getTableName();
        }
        this.functionBuilders = functionBuilders;
        this.pipelineName = pipeline;
    }

    public PipelineBuilder build() {
        PipelineBuilder rootCreator = new PipelineBuilder(namespace, pipelineName);
        if (functionBuilders != null) {
            for (FunctionSQLBuilder functionSqlBuilder : functionBuilders) {
                functionSqlBuilder.setPipelineBuilder(rootCreator);
                functionSqlBuilder.build();
            }
        }
        sqlBuilder.setPipelineBuilder(rootCreator);
        sqlBuilder.buildSql();

        //TODO 拓扑结构在此
        Map<String, List<String>> tree = new HashMap<>();
        Map<AbstractSQLBuilder, BuilderNodeStage> builderBuilderNodeStageMap = new HashMap<>();

        List<AbstractStage<?>> stagesInRoot = rootCreator.getPipeline().getStages();

        if (stagesInRoot != null && stagesInRoot.size() > 0) {
            //这里逻辑处理tumble_processTime.sql中ts as PROCTIME()，语句接收到数据后，
            // 在数据中加入处理时间的逻辑，不然不会有ts这个字段,目前只验证过有一个stagesInRoot.size()=1的情况
            ChainStage last = null;

            for (int i = 0; i < stagesInRoot.size() - 1; i++) {
                AbstractStage current = stagesInRoot.get(i);

                AbstractStage next = stagesInRoot.get(i + 1);

                List<String> labels = new ArrayList<>();

                labels.add(next.getLabel());

                current.setNextStageLabels(labels);
                if (!next.getPrevStageLabels().contains(current.getLabel())) {
                    next.getPrevStageLabels().add(current.getLabel());
                }
            }

            last = (ChainStage) stagesInRoot.get(stagesInRoot.size() - 1);
            //最后一个ChainStage作为下个节点的父节点
            rootCreator.setCurrentChainStage(last);
            build(namespace, sqlBuilder, rootCreator, tree, builderBuilderNodeStageMap);

            ArrayList<String> temp = new ArrayList<>();
            temp.add(last.getLabel());
            //channelNextStageLabel 不为null，window触发后的值才会被计算，不然只会到window触发
            rootCreator.getPipeline().setChannelNextStageLabel(temp);
        } else {
            build(namespace, sqlBuilder, rootCreator, tree, builderBuilderNodeStageMap);
        }

        rootCreator.getPipeline().setMsgSourceName(sqlBuilder.getTableName());
        StringBuilder stringBuilder = new StringBuilder();
        printTree(sqlBuilder.getTableName(), tree, stringBuilder);

        this.rootCreator = rootCreator;
        return this.rootCreator;
    }

    /**
     * 建立一个拓扑结构。核心逻辑是一层一层的创建stage，通过next label建立拓扑关系
     *
     * @param namespace
     * @param sqlBuilder
     * @param currentBuilder
     */
    protected void build(String namespace, AbstractSQLBuilder sqlBuilder, final PipelineBuilder currentBuilder,
                         Map<String, List<String>> tree, Map<AbstractSQLBuilder, BuilderNodeStage> nodeStageMap) {
        List<AbstractSQLBuilder<?>> list = sqlBuilder.getChildren();

        if (CollectionUtil.isEmpty(list)) {
            return;
        }
        List<String> subTables = new ArrayList<>();
        for (AbstractSQLBuilder<?> tmp : list) {
            subTables.add(tmp.getTableName());
        }
        tree.put(sqlBuilder.getTableName(), subTables);
        //一层对应的所有的分支。会从上层接收数据分成多份给每一个分支
        List<ChainStage<?>> chainStages = new ArrayList<>();
        //每个分支最后的输出节点
        List<ChainStage<?>> nextStages = new ArrayList<>();
        //有产生阶段的build，则会进入下一次构建
        List<AbstractSQLBuilder<?>> nextBuilders = new ArrayList<>();
        //对此节点所有child进行编译，并把每个分支最后一个节点形成列表，做递归
        for (AbstractSQLBuilder<?> builder : list) {

            BuilderNodeStage nodeStage = nodeStageMap.get(builder);
            ChainStage first = null;
            ChainStage last = null;
            boolean isBreak = false;
            if (nodeStage == null) {
                PipelineBuilder pipelineBuilder = builderPipeline(builder, sqlBuilder.getTableName());
                isBreak = pipelineBuilder.isBreak();
                List<IConfigurable> configurableList = pipelineBuilder.getConfigurables();
                ChainPipeline chainPipeline = pipelineBuilder.getPipeline();
                configurableList.remove(chainPipeline);
                currentBuilder.addConfigurables(configurableList);
                ChainPipeline pipeline = pipelineBuilder.getPipeline();
                currentBuilder.getPipeline().getStages().addAll(pipeline.getStages());

                if (pipeline.getStages() != null && pipeline.getStages().size() > 0) {
                    List<AbstractStage> stages = pipeline.getStages();
                    for (int i = 0; i < stages.size() - 1; i++) {
                        AbstractStage current = stages.get(i);
                        current.setOwnerSqlNodeTableName(builder.getTableName());
                        AbstractStage next = stages.get(i + 1);
                        current.setMsgSourceName(builder.getTableName());
                        List<String> labels = new ArrayList<>();
                        labels.add(next.getLabel());
                        current.setNextStageLabels(labels);
                        if (!next.getPrevStageLabels().contains(current.getLabel())) {
                            next.getPrevStageLabels().add(current.getLabel());
                        }

                    }
                    first = (ChainStage) pipeline.getStages().get(0);
                    last = (ChainStage) pipeline.getStages().get(pipeline.getStages().size() - 1);
                } else {
                    last = currentBuilder.getCurrentChainStage();
                }

            } else {
                first = nodeStage.firstStage;
                last = nodeStage.lastStage;
            }
            last.setOwnerSqlNodeTableName(builder.getTableName());
            last.setMsgSourceName(builder.getTableName());
            if (!isBreak) {
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
        for (ChainStage stage : nextStages) {
            currentBuilder.setCurrentChainStage(stage);
            AbstractSQLBuilder nextBuilder = nextBuilders.get(i);
            build(namespace, nextBuilder, currentBuilder, tree, nodeStageMap);
            i++;
        }
    }

    /**
     * build pipeline，如果是双流join的右分支，特殊处理，否则正常创建
     *
     * @param builder
     * @return
     */
    protected PipelineBuilder builderPipeline(AbstractSQLBuilder builder, String parentName) {

        PipelineBuilder pipelineBuilder = new PipelineBuilder(namespace, pipelineName);
        pipelineBuilder.setParentTableName(parentName);
        builder.setPipelineBuilder(pipelineBuilder);
        //如果是双流join，且是join中的右流join

        builder.buildSql();

        return pipelineBuilder;
    }

    public PipelineBuilder getRootCreator() {
        return rootCreator;
    }

    public AbstractSQLBuilder getRootBuilder() {
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
        return this.rootCreator.build(configurableService);
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
