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
package com.alibaba.rsqldb.parser;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.rsqldb.parser.builder.AbstractSqlBuilder;

import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.common.model.NameCreatorContext;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.common.topology.metric.StageGroup;
import org.apache.rocketmq.streams.common.topology.model.AbstractChainStage;
import org.apache.rocketmq.streams.common.topology.model.AbstractStage;
import org.apache.rocketmq.streams.common.topology.model.ChainPipeline;
import org.apache.rocketmq.streams.common.topology.stages.EmptyChainStage;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;

public class SqlBuilderResult {
    protected List<IConfigurable> configurables = new ArrayList<>();
    protected List<AbstractStage<?>> stages = new ArrayList<>();
    protected AbstractChainStage<?> firstStage;
    protected AbstractChainStage<?> lastStage;
    protected boolean isRightJoin;

    protected StageGroup stageGroup;

    protected boolean isEmpty = false;

    public SqlBuilderResult(PipelineBuilder pipelineBuilder, AbstractChainStage<?> first, AbstractChainStage<?> lastStage) {
        init(pipelineBuilder, first, lastStage);
    }

    public SqlBuilderResult(PipelineBuilder pipelineBuilder) {
        AbstractChainStage<?> first = CollectionUtil.isNotEmpty(pipelineBuilder.getFirstStages()) ? pipelineBuilder.getFirstStages().get(0) : null;
        init(pipelineBuilder, first, pipelineBuilder.getCurrentChainStage());
    }

    public SqlBuilderResult(PipelineBuilder pipelineBuilder, AbstractSqlBuilder builder) {
        ChainPipeline<?> pipeline = pipelineBuilder.getPipeline();
        if (pipeline.getStages() != null && pipeline.getStages().size() > 0) {
            List<AbstractStage<?>> stages = pipeline.getStages();
            for (int i = 0; i < stages.size() - 1; i++) {
                AbstractStage<?> current = stages.get(i);
                current.setOwnerSqlNodeTableName(builder.getTableName());
                AbstractStage<?> next = stages.get(i + 1);
                List<String> labels = new ArrayList<>();
                labels.add(next.getLabel());
                current.setNextStageLabels(labels);
                if (!next.getPrevStageLabels().contains(current.getLabel())) {
                    next.getPrevStageLabels().add(current.getLabel());
                }

            }
            AbstractChainStage<?> firstStage;
            if (CollectionUtil.isEmpty(pipelineBuilder.getFirstStages())) {
                firstStage = (AbstractChainStage<?>)pipeline.getStages().get(0);
            } else {
                firstStage = pipelineBuilder.getFirstStages().get(0);
            }

            AbstractChainStage<?> lastStage = pipelineBuilder.getCurrentChainStage();
            if (lastStage == null) {
                lastStage = (AbstractChainStage<?>)pipeline.getStages().get(pipeline.getStages().size() - 1);
            }
            init(pipelineBuilder, firstStage, lastStage);
        } else {
            isEmpty = true;
        }
    }

    public boolean isEmpty() {
        return isEmpty;
    }

    public void init(PipelineBuilder pipelineBuilder, AbstractChainStage<?> first, AbstractChainStage<?> lastStage) {
        if (first == null) {
            first = new EmptyChainStage();
            first.setLabel(NameCreatorContext.get().createName("empty"));
        }
        if (lastStage == null) {
            lastStage = first;
        }
        isRightJoin = pipelineBuilder.isRightJoin();
        List<IConfigurable> configurableList = pipelineBuilder.getConfigurables();
        ChainPipeline<?> chainPipeline = pipelineBuilder.getPipeline();
        configurables.addAll(configurableList);
        if (chainPipeline.isTopology()) {
            configurables.remove(chainPipeline);
        }
        stages.addAll(chainPipeline.getStages());
        this.firstStage = first;
        this.lastStage = lastStage;
        StageGroup stageGroup = new StageGroup(firstStage, lastStage, stages);
        if (pipelineBuilder.getCurrentStageGroup() == null) {
            this.stageGroup = stageGroup;
            pipelineBuilder.setCurrentStageGroup(stageGroup);
        } else {
            StageGroup currentStageGroup = pipelineBuilder.getCurrentStageGroup();
            this.stageGroup = currentStageGroup;
            currentStageGroup.setParent(stageGroup);
        }
    }

    public void resetStageGroup() {
        this.stageGroup = new StageGroup(firstStage, lastStage, stages);
    }

    public boolean isRightJoin() {
        return isRightJoin;
    }

    public void setRightJoin(boolean rightJoin) {
        isRightJoin = rightJoin;
    }

    public List<IConfigurable> getConfigurables() {
        return configurables;
    }

    public void setConfigurables(List<IConfigurable> configurables) {
        this.configurables = configurables;
    }

    public AbstractChainStage<?> getFirstStage() {
        return firstStage;
    }

    public void setFirstStage(AbstractChainStage<?> firstStage) {
        this.firstStage = firstStage;
    }

    public AbstractChainStage<?> getLastStage() {
        return lastStage;
    }

    public void setLastStage(AbstractChainStage<?> lastStage) {
        this.lastStage = lastStage;
    }

    public List<AbstractStage<?>> getStages() {
        return stages;
    }

    public void setStages(List<AbstractStage<?>> stages) {
        this.stages = stages;
    }

    public StageGroup getStageGroup() {
        return stageGroup;
    }
}
