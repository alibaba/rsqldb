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
package com.alibaba.rsqldb.clients.sql;

import com.alibaba.rsqldb.parser.sql.ISqlParser;
import com.google.common.collect.Lists;
import java.io.File;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.util.FileUtils;
import org.apache.rocketmq.streams.common.channel.impl.view.ViewSource;
import org.apache.rocketmq.streams.common.channel.source.AbstractSource;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.common.constant.State;
import org.apache.rocketmq.streams.common.model.NameCreatorContext;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.common.topology.assigner.TaskAssigner;
import org.apache.rocketmq.streams.common.topology.model.Pipeline;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.tasks.DispatcherTask;
import org.apache.rocketmq.streams.tasks.StreamTask;

/**
 * @author junjie.cheng
 */
public class DispatcherSqlStream extends AbstractStream<DispatcherSqlStream> {

    protected String taskName;

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public static DispatcherSqlStream create(String namespace) {
        return new DispatcherSqlStream(namespace);
    }

    private DispatcherSqlStream(String namespace) {
        super(namespace);
    }

    public DispatcherSqlStream name(String rootTaskName) {
        if (this.taskName == null) {
            this.taskName = rootTaskName;
        }
        // 更新streamsTask
        DispatcherTask streamTask = this.configurableComponent.loadConfigurableFromStorage(DispatcherTask.TYPE, this.taskName, this.namespace);
        if (streamTask == null) {
            streamTask = new DispatcherTask(this.namespace, this.taskName);
            streamTask.setNameSpace(this.namespace);
            streamTask.setConfigureName(this.taskName);
            streamTask.setUpdateFlag(0);
            streamTask.setState(State.STARTED);
            this.configurableComponent.insert(streamTask);
        }
        return this;
    }

    public DispatcherSqlStream sqlPath(String name, String sqlPath) throws Exception {
        String sql = FileUtils.readFile(new File(sqlPath), "utf-8");
        return sql(name, sql);
    }

    public DispatcherSqlStream sql(String name, String sql) throws Exception {
        return init(name, sql, 1);
    }

    public DispatcherSqlStream sql(String name, String sql, Integer weight) throws Exception {
        return init(name, sql, weight);
    }

    private DispatcherSqlStream init(String name, String sql, Integer weight) {
        ISqlParser sqlParser = parse(name, sql);
        if (weight == 0) {
            weight = getWeight(sqlParser);
        }
        //更新configurables 和 pipelines
        for (IConfigurable iConfigurable : sqlParser.configurables()) {
            BasedConfigurable configurable = (BasedConfigurable) iConfigurable;
            configurable.setUpdateFlag(System.currentTimeMillis());
            if (configurable instanceof Pipeline) {
                Pipeline<?> pipeline = this.configurableComponent.loadConfigurableFromStorage(Pipeline.TYPE, configurable.getConfigureName(), this.namespace);
                if (pipeline != null) {
                    configurable.setState(pipeline.getState());
                } else {
                    configurable.setState(State.STOPPED);
                }
            }
            this.configurableComponent.insertToCache(configurable);
        }
        //更新task assigner
        List<ChainPipeline<?>> pipelines = sqlParser.pipelines().stream().filter(pipeline -> !(pipeline.getSource() instanceof ViewSource)).collect(Collectors.toList());
        for (ChainPipeline<?> pipeline : pipelines) {
            String pipelineName = pipeline.getConfigureName();
            TaskAssigner taskAssigner = new TaskAssigner();
            taskAssigner.setTaskName(this.taskName);
            taskAssigner.setPipelineName(pipelineName);
            taskAssigner.setNameSpace(this.namespace);
            taskAssigner.setType(TaskAssigner.TYPE);
            taskAssigner.setConfigureName(NameCreatorContext.get().createOrGet(pipelineName).createName(pipelineName, TaskAssigner.TYPE));
            taskAssigner.setWeight(weight);
            taskAssigner.setUpdateFlag(System.currentTimeMillis());
            this.configurableComponent.insertToCache(taskAssigner);
        }
        this.configurableComponent.flushCache();
        return this;
    }

    public ISqlParser parse(String name, String sql) {
        return createSqlTreeBuilder(this.namespace, name, sql, this.configurableComponent).parse(false);
    }

    public Integer getWeight(ISqlParser sqlParser) {
        List<IConfigurable> configurables = sqlParser.configurables();
        for (IConfigurable configurable : configurables) {
            if (configurable instanceof AbstractSource) {
                List<ISplit<?, ?>> splits = ((AbstractSource) configurable).getAllSplits();
                if (splits != null) {
                    return splits.size();
                }
            }
        }
        return 1;
    }

    @Override public void start() throws Exception {
        DispatcherTask dispatcherTask = this.configurableComponent.loadConfigurableFromStorage(DispatcherTask.TYPE, this.taskName, this.namespace);
        String currentState = State.STOPPED;
        if (dispatcherTask != null && currentState.equals(dispatcherTask.getState())) {
            DispatcherTask copy = ReflectUtil.forInstance(dispatcherTask.getClass());
            copy.toObject(dispatcherTask.toJson());
            copy.setState(State.STARTED);
            copy.setUpdateFlag(System.currentTimeMillis());
            this.configurableComponent.insert(copy);
        }
    }

    public void startPipeline(String pipelineName) {
        ChainPipeline<?> chainPipeline = this.configurableComponent.loadConfigurableFromStorage(Pipeline.TYPE, pipelineName, this.namespace);
        String currentState = State.STOPPED;
        if (chainPipeline != null && currentState.equals(chainPipeline.getState())) {
            ChainPipeline<?> copy = ReflectUtil.forInstance(chainPipeline.getClass());
            copy.toObject(chainPipeline.toJson());
            copy.setState(State.STARTED);

            //copy.setUpdateFlag(chainPipeline.getUpdateFlag() + 1);  //启动、停止操作由外部接口完成，无需自动加载
            this.configurableComponent.insert(copy);
        }
    }

    @Override public void stop() throws Exception {
        DispatcherTask dispatcherTask = this.configurableComponent.loadConfigurableFromStorage(DispatcherTask.TYPE, this.taskName, this.namespace);
        String currentState = State.STARTED;
        if (dispatcherTask != null && currentState.equals(dispatcherTask.getState())) {
            DispatcherTask copy = ReflectUtil.forInstance(dispatcherTask.getClass());
            copy.toObject(dispatcherTask.toJson());
            copy.setState(State.STOPPED);
            copy.setUpdateFlag(dispatcherTask.getUpdateFlag() + 1);
            this.configurableComponent.insert(copy);
        }
    }

    public void stopPipeline(String pipelineName) {
        ChainPipeline<?> chainPipeline = this.configurableComponent.loadConfigurableFromStorage(Pipeline.TYPE, pipelineName, this.namespace);
        String currentState = State.STARTED;
        if (chainPipeline != null && currentState.equals(chainPipeline.getState())) {
            ChainPipeline<?> copy = ReflectUtil.forInstance(chainPipeline.getClass());
            copy.toObject(chainPipeline.toJson());
            copy.setState(State.STOPPED);
            //copy.setUpdateFlag(chainPipeline.getUpdateFlag() + 1); //启动停止操作有外部接口完成
            this.configurableComponent.insert(copy);
        }
    }

    public void drop() throws Exception {
        DispatcherTask dispatcherTask = this.configurableComponent.loadConfigurableFromStorage(DispatcherTask.TYPE, this.taskName, this.namespace);
        String currentState = State.STOPPED;
        if (dispatcherTask != null && currentState.equals(dispatcherTask.getState())) {
            DispatcherTask copy = ReflectUtil.forInstance(dispatcherTask.getClass());
            copy.toObject(dispatcherTask.toJson());
            copy.close();
            copy.setUpdateFlag(dispatcherTask.getUpdateFlag() + 1);
            this.configurableComponent.insert(copy);
        }
    }

    public void dropPipeline(String pipelineName) {
        ChainPipeline<?> chainPipeline = this.configurableComponent.loadConfigurableFromStorage(Pipeline.TYPE, pipelineName, this.namespace);
        String currentState = State.STOPPED;
        if (chainPipeline != null && currentState.equals(chainPipeline.getState())) {
            ChainPipeline<?> copy = ReflectUtil.forInstance(chainPipeline.getClass());
            copy.toObject(chainPipeline.toJson());
            copy.close();
            copy.setUpdateFlag(chainPipeline.getUpdateFlag() + 1);
            this.configurableComponent.insert(copy);
        }
    }

    @Override public List<StreamTask> list() throws Exception {
        return this.configurableComponent.loadConfigurableFromStorage(DispatcherTask.TYPE, this.namespace);
    }

    public List<ChainPipeline<?>> listPipelines() {
        List<ChainPipeline<?>> chainPipelines = Lists.newArrayList();
        List<TaskAssigner> assigners = this.configurableComponent.loadConfigurableFromStorage(TaskAssigner.TYPE, this.namespace);
        if (assigners == null) {
            assigners = Lists.newArrayList();
        } else {
            assigners = assigners.stream().filter(taskAssigner -> taskAssigner.getTaskName().equals(this.taskName)).collect(Collectors.toList());
        }
        for (TaskAssigner taskAssigner : assigners) {
            ChainPipeline<?> pipeline = this.configurableComponent.loadConfigurableFromStorage(Pipeline.TYPE, taskAssigner.getPipelineName(), this.namespace);
            if (pipeline != null) {
                chainPipelines.add(pipeline);
            }
        }
        return chainPipelines;
    }

}
