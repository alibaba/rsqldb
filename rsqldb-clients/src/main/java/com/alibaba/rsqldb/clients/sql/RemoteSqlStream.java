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
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.util.FileUtils;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.common.constant.State;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.configurable.ConfigurableComponent;
import org.apache.rocketmq.streams.tasks.StreamTask;

public class RemoteSqlStream extends AbstractStream<RemoteSqlStream> {

    protected String taskName;

    public static RemoteSqlStream create(String namespace) {
        return new RemoteSqlStream(namespace);
    }

    private RemoteSqlStream(String namespace) {
        super(namespace);
    }

    public RemoteSqlStream name(String taskName) {
        if (this.configurableComponent == null) {
            throw new RuntimeException("you must execute init before execute name function ");
        }
        this.taskName = taskName;
        return this;
    }

    public RemoteSqlStream sqlPath(String sqlPath) throws Exception {
        String sql = FileUtils.readFile(new File(sqlPath), "utf-8");
        return sql(sql);
    }

    public RemoteSqlStream sql(String sql) {
        if (this.configurableComponent == null) {
            throw new RuntimeException("you must execute init before execute sql function ");
        }
        if (this.taskName == null) {
            throw new RuntimeException("you must execute name before execute sql function ");
        }
        ISqlParser sqlParser = createSqlTreeBuilder(namespace, taskName, sql, this.configurableComponent).parse(false);
        long updateVersion = System.currentTimeMillis();
        for (IConfigurable iConfigurable : sqlParser.configurables()) {
            if (iConfigurable instanceof BasedConfigurable) {
                BasedConfigurable abstractConfigurable = (BasedConfigurable) iConfigurable;
                abstractConfigurable.setVersion(updateVersion + "");
            }
            this.configurableComponent.insertToCache(iConfigurable);
        }
        List<ChainPipeline<?>> pipelines = sqlParser.pipelines();
        StreamTask streamTask = new StreamTask();
        streamTask.setNameSpace(this.namespace);
        streamTask.setConfigureName(this.taskName);
        streamTask.setUpdateFlag(0);
        streamTask.setPipelines(pipelines);
//        streamTask.setNeedThread(true);
        this.configurableComponent.insertToCache(streamTask);
        this.configurableComponent.flushCache();
        return this;
    }

    public void startStreamTasks() {

        List<StreamTask> tasks = this.configurableComponent.queryConfigurableByType(StreamTask.TYPE);
        if (tasks != null) {
            for (StreamTask streamTask : tasks) {
                streamTask.start();
            }
        }
    }

    @Override public void start() throws Exception {
        if (this.configurableComponent == null) {
            throw new Exception("you must execute init before execute start function ");
        }
        if (this.taskName == null) {
            throw new Exception("you must execute name before execute start function ");
        }
        this.configurableComponent.refreshConfigurable(namespace);
        StreamTask streamTask = get(this.configurableComponent, taskName);
        String currentState = State.STOPPED;
        if (streamTask != null && currentState.equals(streamTask.getState())) {
            StreamTask copy = ReflectUtil.forInstance(streamTask.getClass());
            copy.toObject(streamTask.toJson());
            copy.setState(State.STARTED);
            copy.setUpdateFlag(streamTask.getUpdateFlag() + 1);
            this.configurableComponent.insert(copy);
        }
    }

    @Override public void stop() throws Exception {
        if (this.configurableComponent == null) {
            throw new Exception("you must execute init before execute stop function ");
        }
        if (this.taskName == null) {
            throw new Exception("you must execute name before execute stop function ");
        }
        this.configurableComponent.refreshConfigurable(namespace);
        StreamTask streamTask = get(this.configurableComponent, taskName);
        String currentState = State.STARTED;
        if (streamTask != null && currentState.equals(streamTask.getState())) {
            StreamTask copy = ReflectUtil.forInstance(streamTask.getClass());
            copy.toObject(streamTask.toJson());
            copy.setState(State.STOPPED);
            copy.setUpdateFlag(streamTask.getUpdateFlag() + 1);
            this.configurableComponent.insert(copy);
        }
    }

    @Override public List<StreamTask> list() throws Exception {
        if (this.configurableComponent == null) {
            throw new Exception("you must execute init before execute list function ");
        }
        this.configurableComponent.refreshConfigurable(namespace);
        return this.configurableComponent.queryConfigurableByType(StreamTask.TYPE);
    }

    private StreamTask get(ConfigurableComponent configurableComponent, String taskName) throws Exception {
        StreamTask streamsTask = null;
        List<StreamTask> streamsTasks = configurableComponent.queryConfigurableByType(StreamTask.TYPE);
        if (taskName != null) {
            List<String> jobNames = Arrays.asList(taskName.split(","));
            streamsTasks = streamsTasks.stream().filter(task -> jobNames.contains(task.getConfigureName())).collect(Collectors.toList());
        }
        if (streamsTasks != null && !streamsTasks.isEmpty()) {
            streamsTask = streamsTasks.get(0);
        }
        return streamsTask;
    }

}
