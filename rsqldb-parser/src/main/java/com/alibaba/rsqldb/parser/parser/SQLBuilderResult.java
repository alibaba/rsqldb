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
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.common.topology.ChainStage;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.common.topology.metric.StageGroup;
import org.apache.rocketmq.streams.common.topology.model.AbstractStage;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;

public class SQLBuilderResult {
    protected List<IConfigurable> configurables=new ArrayList<>();
    protected List<AbstractStage<?>> stages=new ArrayList<>();
    protected ChainStage firstStage;
    protected ChainStage lastStage;
    protected boolean isRightJoin;

    protected StageGroup stageGroup;
    public SQLBuilderResult(PipelineBuilder pipelineBuilder, ChainStage<?> first,ChainStage<?> lastStage){
        init(pipelineBuilder,first,lastStage);
    }

    public SQLBuilderResult(PipelineBuilder pipelineBuilder){
        ChainStage<?> first=CollectionUtil.isNotEmpty(pipelineBuilder.getFirstStages())?pipelineBuilder.getFirstStages().get(0):null;
        init(pipelineBuilder,first,pipelineBuilder.getCurrentChainStage());
    }

    public SQLBuilderResult(PipelineBuilder pipelineBuilder, AbstractSQLBuilder builder){
        ChainPipeline pipeline = pipelineBuilder.getPipeline();
        if (pipeline.getStages() != null && pipeline.getStages().size() > 0) {
            List<AbstractStage> stages = pipeline.getStages();
            for (int i = 0; i < stages.size() - 1; i++) {
                AbstractStage current = stages.get(i);
                current.setOwnerSqlNodeTableName(builder.getTableName());
                AbstractStage next = stages.get(i + 1);
               // current.setMsgSourceName(builder.getTableName());
                List<String> labels = new ArrayList<>();
                labels.add(next.getLabel());
                current.setNextStageLabels(labels);
                if (!next.getPrevStageLabels().contains(current.getLabel())) {
                    next.getPrevStageLabels().add(current.getLabel());
                }

            }
            ChainStage firstStage;
            if(CollectionUtil.isEmpty(pipelineBuilder.getFirstStages())){
                firstStage = (ChainStage) pipeline.getStages().get(0);
            }else {
                firstStage=pipelineBuilder.getFirstStages().get(0);
            }

            ChainStage lastStage=pipelineBuilder.getCurrentChainStage();
            if(lastStage==null){
                lastStage = (ChainStage) pipeline.getStages().get(pipeline.getStages().size() - 1);
            }
            init(pipelineBuilder,firstStage,lastStage);
        }
    }


    public void  init(PipelineBuilder pipelineBuilder, ChainStage first,ChainStage lastStage){
        isRightJoin = pipelineBuilder.isRightJoin();
        List<IConfigurable> configurableList = pipelineBuilder.getConfigurables();
        ChainPipeline chainPipeline = pipelineBuilder.getPipeline();
        configurables.addAll(configurableList);
        if(chainPipeline.isTopology()){
            configurables.remove(chainPipeline);
        }
        stages.addAll(chainPipeline.getStages());
        this.firstStage=first;
        this.lastStage=lastStage;
        if(pipelineBuilder.getCurrentStageGroup()==null){
            stageGroup=new StageGroup(firstStage,lastStage,stages);
            pipelineBuilder.setCurrentStageGroup(stageGroup);
        }else {
            stageGroup=pipelineBuilder.getCurrentStageGroup();
        }
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

    public ChainStage getFirstStage() {
        return firstStage;
    }

    public void setFirstStage(ChainStage firstStage) {
        this.firstStage = firstStage;
    }

    public ChainStage getLastStage() {
        return lastStage;
    }

    public void setLastStage(ChainStage lastStage) {
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
