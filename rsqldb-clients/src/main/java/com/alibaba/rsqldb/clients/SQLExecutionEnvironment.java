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
package com.alibaba.rsqldb.clients;

import java.io.File;
import java.util.Properties;

import com.alibaba.rsqldb.clients.dispather.DispatcherJobGraph;
import com.alibaba.rsqldb.clients.merge.MergeSQLJobGraph;
import com.alibaba.rsqldb.parser.sql.ISqlParser;
import com.alibaba.rsqldb.parser.sql.SqlParserProvider;

import org.apache.rocketmq.streams.client.AbstractExecutionEnvironment;
import org.apache.rocketmq.streams.common.configuration.JobConfiguration;
import org.apache.rocketmq.streams.common.topology.graph.JobGraph;
import org.apache.rocketmq.streams.common.utils.FileUtil;
import org.apache.rocketmq.streams.dispatcher.IDispatcherCallback;

public class SQLExecutionEnvironment extends AbstractExecutionEnvironment<SQLExecutionEnvironment> {

    private SQLExecutionEnvironment() {
        super();
    }

    public static SQLExecutionEnvironment getExecutionEnvironment() {
        return SQLExecutionEnvironmentFactory.Instance;
    }

    public JobGraph createJob(String namespace, String jobName, String sql) {
        ISqlParser sqlParser = SqlParserProvider.create();
        return sqlParser.parse(namespace, jobName, sql, this.getProperties());
    }

    public JobGraph createJob(String namespace, String jobName, String sql, JobConfiguration jobConfiguration) {
        ISqlParser sqlParser = SqlParserProvider.create();
        Properties properties = this.getProperties();
        properties.putAll(jobConfiguration.getProperties());
        return sqlParser.parse(namespace, jobName, sql, properties);
    }

    public JobGraph createJobFromPath(String namespace, String jobName, String sqlPath) {
        String sql = FileUtil.loadFileContentContainLineSign(sqlPath);
        return createJob(namespace, jobName, sql);
    }

    public JobGraph createJobFromPath(String namespace, String jobName, String sqlPath, JobConfiguration jobConfiguration) {
        String sql = FileUtil.loadFileContentContainLineSign(sqlPath);
        return createJob(namespace, jobName, sql, jobConfiguration);
    }

    public JobGraph createJobFromPath(String namespace, String sqlPath) {
        String sql = FileUtil.loadFileContentContainLineSign(sqlPath);
        String jobName = getJobName(sqlPath);
        return createJob(namespace, jobName, sql);
    }

    public JobGraph createJobFromPath(String namespace, String sqlPath, JobConfiguration jobConfiguration) {
        String sql = FileUtil.loadFileContentContainLineSign(sqlPath);
        String jobName = getJobName(sqlPath);
        return createJob(namespace, jobName, sql, jobConfiguration);
    }

    private String getJobName(String sqlPath) {
        String jobName = null;
        if (sqlPath.contains(File.separator)) {
            int lastIndex = sqlPath.lastIndexOf(File.separator);
            if (lastIndex != -1) {
                jobName = sqlPath.substring(lastIndex + 1).replaceAll("\\.", "_");
            }
        }
        return jobName;
    }

    /**
     * 同源任务归并在一起，只消费一次数据源数据
     *
     * @param namespace 命名空间
     * @return mergeJob
     */
    public MergeSQLJobGraph createMergeSQLJob(String namespace, String jobName, String sql) {
        JobGraph jobGraph = createJob(namespace, jobName, sql);
        return new MergeSQLJobGraph(this, jobGraph);
    }

    public MergeSQLJobGraph createMergeSQLJob(String namespace, String jobName, String sql, JobConfiguration jobConfiguration) {
        JobGraph jobGraph = createJob(namespace, jobName, sql, jobConfiguration);
        return new MergeSQLJobGraph(this, jobGraph);
    }

    /**
     * 同源任务归并在一起，只消费一次数据源数据
     *
     * @param namespace 命名空间
     * @param sqlPath   sql路径
     * @return merge job
     */
    public MergeSQLJobGraph createMergeSQLJobFromPath(String namespace, String sqlPath) {
        JobGraph jobGraph = createJobFromPath(namespace, sqlPath);
        return new MergeSQLJobGraph(this, jobGraph);
    }

    public MergeSQLJobGraph createMergeSQLJobFromPath(String namespace, String sqlPath, JobConfiguration jobConfiguration) {
        JobGraph jobGraph = createJobFromPath(namespace, sqlPath, jobConfiguration);
        return new MergeSQLJobGraph(this, jobGraph);
    }

    /**
     * 很多小任务的场景，多个小任务调度到部分资源运行
     *
     * @param namespace          命名空间
     * @param dispatcherCallback 回调函数
     * @return 调度任务
     */
    public DispatcherJobGraph createDispatcherJob(String namespace, IDispatcherCallback<?> dispatcherCallback) {
        return new DispatcherJobGraph(namespace, this.getProperties(), dispatcherCallback);
    }

    /**
     * 多任务调度
     *
     * @param namespace          命名空间
     * @param jobConfiguration   配置
     * @param dispatcherCallback 回调函数
     * @return 调度任务
     */
    public DispatcherJobGraph createDispatcherJob(String namespace, IDispatcherCallback<?> dispatcherCallback, JobConfiguration jobConfiguration) {
        Properties properties = this.getProperties();
        properties.putAll(jobConfiguration.getProperties());
        return new DispatcherJobGraph(namespace, properties, dispatcherCallback);
    }

    private static class SQLExecutionEnvironmentFactory {
        private static final SQLExecutionEnvironment Instance = new SQLExecutionEnvironment();
    }

}
