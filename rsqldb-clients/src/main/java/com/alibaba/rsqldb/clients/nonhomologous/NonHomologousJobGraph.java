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
package com.alibaba.rsqldb.clients.nonhomologous;

import java.util.List;
import java.util.Properties;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.rsqldb.clients.SQLExecutionEnvironment;

import org.apache.rocketmq.streams.common.channel.impl.view.ViewSource;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.interfaces.IStreamOperator;
import org.apache.rocketmq.streams.common.topology.graph.JobGraph;
import org.apache.rocketmq.streams.connectors.source.impl.NonHomologousPullSource;

public class NonHomologousJobGraph extends JobGraph {

    protected NonHomologousPullSource source;
    protected SQLExecutionEnvironment evn;

    public NonHomologousJobGraph(SQLExecutionEnvironment evn, String namespace, String jobName,
        Properties properties) {
        super(namespace, jobName, null, properties);
        this.evn = evn;
    }

    @Override
    public void start() {
        source.start(new IStreamOperator() {
            @Override
            public Object doMessage(IMessage message, AbstractContext context) {
                return null;
            }
        });
    }

    @Override
    public void stop() {
        source.destroy();
    }

    @Override
    public List<JSONObject> execute(List<JSONObject> msgs) {
        throw new RuntimeException("can not support this metho");
    }

    /**
     * 创建子 sql
     *
     * @param sqlFilePath
     * @return
     */
    public JobGraph createJobFromPath(String sqlFilePath) {
        JobGraph jobGraph = this.evn.createJobFromPath(getNamespace(), sqlFilePath);
        return new JobGraphProxy(jobGraph);
    }

    /**
     * 创建子 sql
     *
     * @param jobName
     * @param sql
     * @return
     */
    public JobGraph createJob(String jobName, String sql) {
        JobGraph jobGraph = this.evn.createJob(getNamespace(), jobName, sql);
        return new JobGraphProxy(jobGraph);
    }

    public class JobGraphProxy extends JobGraph {

        protected JobGraph jobGraph;

        public JobGraphProxy(JobGraph jobGraph) {
            super(jobGraph.getNamespace(), jobGraph.getJobName(), jobGraph.getPipelines());
            this.jobGraph = jobGraph;
            if (!(jobGraph.getPipelines().get(0).getSource() instanceof ViewSource)) {
                throw new RuntimeException("The sql source must view type" + jobGraph.getJobName());
            }
            if (jobGraph.getJobConfiguration() != null) {
                this.setJobConfiguration(jobGraph.getJobConfiguration());
            }

        }

        @Override
        public void start() {
            source.addSubPipeline(jobGraph.getPipelines().get(0));
        }

        @Override
        public void stop() {
            source.removeSubPipeline(jobGraph.getPipelines().get(0).getName());
        }
    }
}
