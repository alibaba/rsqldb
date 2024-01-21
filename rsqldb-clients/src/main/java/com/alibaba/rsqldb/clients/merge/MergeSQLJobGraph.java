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
package com.alibaba.rsqldb.clients.merge;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.rsqldb.clients.SQLExecutionEnvironment;
import com.alibaba.rsqldb.parser.sql.stage.MergeSQLJobStage;

import org.apache.rocketmq.streams.common.channel.impl.view.ViewSource;
import org.apache.rocketmq.streams.common.channel.source.AbstractSource;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.topology.graph.JobGraph;
import org.apache.rocketmq.streams.common.topology.model.AbstractStage;
import org.apache.rocketmq.streams.common.topology.model.ChainPipeline;

/**
 * 支持merge sql，一个主sql，把数据写入调view 类型的sink，其他sql基于view 作为数据源
 */
public class MergeSQLJobGraph extends JobGraph {
    protected SQLExecutionEnvironment evn;
    protected String namespace;

    protected JobGraph jobGraph;
    protected MergeSQLJobStage mergeSQLJobStage;
    protected AbstractSource mergeSource;
    protected ConcurrentHashMap<String, JobGraph> subJobGraphs = new ConcurrentHashMap<>();

    public MergeSQLJobGraph(SQLExecutionEnvironment evn, JobGraph jobGraph) {
        super(jobGraph.getNamespace(), jobGraph.getJobName(), jobGraph.getPipelines());
        this.jobGraph = jobGraph;
        this.evn = evn;
        this.mergeSQLJobStage = getMergeSQLJobStage(jobGraph);
        this.namespace = jobGraph.getNamespace();
        ChainPipeline chainPipeline = jobGraph.getPipelines().get(0);
        mergeSource = (AbstractSource)chainPipeline.getSource();
    }

    public MergeSQLJobGraph(SQLExecutionEnvironment evn, JobGraph jobGraph, Properties jobConfiguration) {
        super(jobGraph.getNamespace(), jobGraph.getJobName(), jobGraph.getPipelines(), jobConfiguration);
        this.jobGraph = jobGraph;
        this.mergeSQLJobStage = getMergeSQLJobStage(jobGraph);
        this.evn = evn;
        this.namespace = jobGraph.getNamespace();
    }

    @Override
    public void start() {
        for (JobGraph subJobGraph : this.subJobGraphs.values()) {
            subJobGraph.start();
        }
        mergeSQLJobStage.setSubJobGraphs(subJobGraphs.values());
        jobGraph.start();
    }

    @Override
    public void stop() {
        jobGraph.stop();
        for (JobGraph subJobGraph : this.subJobGraphs.values()) {
            subJobGraph.stop();
        }
    }

    /**
     * 创建子 sql
     *
     * @param sqlFilePath
     * @return
     */
    public JobGraph createJobFromPath(String sqlFilePath) {
        JobGraph jobGraph = this.evn.createJobFromPath(namespace, sqlFilePath);
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
        JobGraph jobGraph = this.evn.createJob(namespace, jobName, sql);
        return new JobGraphProxy(jobGraph);
    }

    @Override
    public List<JSONObject> execute(List<JSONObject> msgs) {
        return super.execute(msgs);
    }

    protected MergeSQLJobStage getMergeSQLJobStage(JobGraph jobGraph) {
        List<AbstractStage<?>> stages = jobGraph.getPipelines().get(0).getStages();
        for (AbstractStage<?> stage : stages) {
            if (stage instanceof MergeSQLJobStage) {
                return (MergeSQLJobStage)stage;
            }
        }
        throw new RuntimeException("The sql's sink must view type");
    }

    public class JobGraphProxy extends JobGraph {

        protected JobGraph jobGraph;

        public JobGraphProxy(JobGraph jobGraph) {
            super(jobGraph.getNamespace(), jobGraph.getJobName(), jobGraph.getPipelines());
            this.jobGraph = jobGraph;
            if (!(jobGraph.getPipelines().get(0).getSource() instanceof ViewSource)) {
                throw new RuntimeException("The sql source must view type" + jobGraph.getJobName());
            }
            if (jobGraph.getPipelines().size() != 1) {
                throw new RuntimeException("MergeSQLExecutionEnvironment can not support double flow， please user SQLExecutionEnvironment submit sql");
            }
            if (jobGraph.getJobConfiguration() != null) {
                this.setJobConfiguration(jobGraph.getJobConfiguration());
            }
            subJobGraphs.put(jobGraph.getJobName(), jobGraph);
            mergeSQLJobStage.setSubJobGraphs(subJobGraphs.values());
        }

        @Override
        public void start() {
            if (mergeSQLJobStage != null) {
                ChainPipeline chainPipeline = (ChainPipeline)jobGraph.getPipelines().get(0);
                ISource source = chainPipeline.getSource();
                if (source instanceof ViewSource) {
                    ViewSource viewSource = (ViewSource)source;
                    viewSource.setRootSource(mergeSource);
                }
                jobGraph.start();

            }

        }

        @Override
        public void stop() {
            subJobGraphs.remove(jobGraph.getJobName());
            if (mergeSQLJobStage != null) {
                mergeSQLJobStage.setSubJobGraphs(subJobGraphs.values());
            }
            jobGraph.stop();
        }
    }

}
