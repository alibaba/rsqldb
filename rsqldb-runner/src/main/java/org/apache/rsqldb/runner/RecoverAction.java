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
package org.apache.rsqldb.runner;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.common.topology.model.Pipeline;
import org.apache.rocketmq.streams.common.topology.task.StreamsTask;
import org.apache.rocketmq.streams.configurable.ConfigurableComponent;

/**
 * @author junjie.cheng
 * @date 2021/11/25
 */
public class RecoverAction {

    public static void main(String[] args) {
        if (args.length < 1) {
            return;
        }
        String namespace = null;
        if (args.length > 0) {
            namespace = args[0];
        }
        String jobName = null;
        if (args.length > 1) {
            jobName = args[1];
        }
        List<ChainPipeline> runningPipelines = Lists.newArrayList();
        if (namespace == null && jobName == null) {
            return;
        } else if (namespace != null) {
            ConfigurableComponent configurableComponent = ComponentCreator.getComponent(namespace, ConfigurableComponent.class);
            runningPipelines = configurableComponent.queryConfigurableByType(Pipeline.TYPE);
            if (runningPipelines.isEmpty()) {
                runningPipelines = configurableComponent.queryConfigurableByType(StreamsTask.TYPE);
            }
            if (jobName != null) {
                List<String> jobNames = Arrays.asList(jobName.split(","));
                runningPipelines = runningPipelines.stream().filter(pipeline -> jobNames.contains(pipeline.getConfigureName())).collect(Collectors.toList());
            }
        }
        if (!runningPipelines.isEmpty()) {
            StreamsTask job = new StreamsTask();
            job.setNameSpace(namespace);
            job.setPipelines(runningPipelines);
            job.start();
        }

    }

}
