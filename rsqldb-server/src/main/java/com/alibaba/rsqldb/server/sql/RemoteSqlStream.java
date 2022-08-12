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

package com.alibaba.rsqldb.server.sql;

import com.alibaba.rsqldb.parser.builder.SqlTreeBuilder;
import org.apache.flink.util.FileUtils;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.common.topology.task.StreamsTask;
import org.apache.rocketmq.streams.configurable.ConfigurableComponent;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class RemoteSqlStream extends AbstractStream<RemoteSqlStream> {

    protected String namespace;
    protected String taskName;

    public static RemoteSqlStream create(String namespace) {
        return new RemoteSqlStream(namespace);
    }

    private RemoteSqlStream(String namespace) {
        this.namespace = namespace;
    }

    protected ConfigurableComponent configurableComponent;

    public RemoteSqlStream init() {
        ComponentCreator.updateProperties(this.properties);
        this.configurableComponent = ComponentCreator.getComponentNotStart(this.namespace, ConfigurableComponent.class);
        this.configurableComponent.refreshConfigurable(namespace);
        return this;
    }

    public RemoteSqlStream name(String taskName) throws Exception {
        if (this.configurableComponent == null) {
            throw new Exception("you must execute init before execute name function ");
        }
        this.taskName = taskName;
        return this;
    }

    public RemoteSqlStream sqlPath(String sqlPath) throws Exception {
        String sql = FileUtils.readFile(new File(sqlPath), "utf-8");
        return sql(sql);
    }

    public RemoteSqlStream sql(String sql) throws Exception {
        if (this.configurableComponent == null) {
            throw new Exception("you must execute init before execute sql function ");
        }
        if (this.taskName == null) {
            throw new Exception("you must execute name before execute sql function ");
        }
        SqlTreeBuilder sqlTreeBuilder = new SqlTreeBuilder(namespace, taskName, sql, this.configurableComponent);
        //更新其他实例
        List<IConfigurable> configurables = sqlTreeBuilder.build(false);
        for (IConfigurable iConfigurable : configurables) {
            BasedConfigurable oldConfigurable = (BasedConfigurable) this.configurableComponent.queryConfigurableByIdent(iConfigurable.getType(), iConfigurable.getConfigureName());
            if (oldConfigurable == null) {
                ((BasedConfigurable) iConfigurable).setUpdateFlag(0);
            } else {
                ((BasedConfigurable) iConfigurable).setUpdateFlag(oldConfigurable.getUpdateFlag() + 1);
            }
            iConfigurable.setConfigureName(iConfigurable.getConfigureName());
            iConfigurable.setNameSpace(iConfigurable.getNameSpace());
            iConfigurable.setType(iConfigurable.getType());
            iConfigurable.toObject(iConfigurable.toJson());
            this.configurableComponent.insert(iConfigurable);
        }
        this.configurableComponent.refreshConfigurable(namespace);
        return this;
    }

    public void start() throws Exception {
        if (this.configurableComponent == null) {
            throw new Exception("you must execute init before execute start function ");
        }
        if (this.taskName == null) {
            throw new Exception("you must execute name before execute start function ");
        }
        StreamsTask streamsTask = get(this.configurableComponent, taskName);
        String currentState = StreamsTask.STATE_STOPPED;
        if (streamsTask != null && currentState.equals(streamsTask.getState())) {
            StreamsTask copy = new StreamsTask();
            copy.toObject(streamsTask.toJson());
            copy.setState(StreamsTask.STATE_STARTED);
            copy.setUpdateFlag(streamsTask.getUpdateFlag() + 1);
            this.configurableComponent.insert(copy);
        }
    }

    public void stop() throws Exception {
        if (this.configurableComponent == null) {
            throw new Exception("you must execute init before execute stop function ");
        }
        if (this.taskName == null) {
            throw new Exception("you must execute name before execute stop function ");
        }
        StreamsTask streamsTask = get(this.configurableComponent, taskName);
        String currentState = StreamsTask.STATE_STARTED;
        if (streamsTask != null && currentState.equals(streamsTask.getState())) {
            StreamsTask copy = new StreamsTask();
            copy.toObject(streamsTask.toJson());
            copy.setState(StreamsTask.STATE_STOPPED);
            copy.setUpdateFlag(streamsTask.getUpdateFlag() + 1);
            this.configurableComponent.insert(copy);
        }
    }

    public List<StreamsTask> list() throws Exception {
        if (this.configurableComponent == null) {
            throw new Exception("you must execute init before execute list function ");
        }
        return this.configurableComponent.queryConfigurableByType(StreamsTask.TYPE);
    }

    private StreamsTask get(ConfigurableComponent configurableComponent, String taskName) throws Exception {
        StreamsTask streamsTask = null;
        List<StreamsTask> streamsTasks = configurableComponent.queryConfigurableByType(StreamsTask.TYPE);
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
