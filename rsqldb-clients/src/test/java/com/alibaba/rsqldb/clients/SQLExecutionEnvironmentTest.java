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

import java.util.List;

import com.alibaba.rsqldb.clients.dispather.DispatcherJobGraph;
import com.alibaba.rsqldb.clients.merge.MergeSQLJobGraph;

import org.apache.rocketmq.streams.common.configuration.JobConfiguration;
import org.apache.rocketmq.streams.common.topology.graph.JobGraph;
import org.apache.rocketmq.streams.common.topology.model.ChainPipeline;
import org.apache.rocketmq.streams.dispatcher.callback.DefaultDispatcherCallback;
import org.junit.Before;
import org.junit.Test;

public class SQLExecutionEnvironmentTest {

    private SQLExecutionEnvironment sqlExecutionEnvironment;

    @Before
    public void init() {
        sqlExecutionEnvironment = SQLExecutionEnvironment.getExecutionEnvironment().db("", "", "", "");

    }

    @Test
    public void testFilter() throws InterruptedException {
        JobConfiguration jobConfiguration = new JobConfiguration();
        jobConfiguration.config("filepath", "window_msg_10000.txt");
        sqlExecutionEnvironment.createJobFromPath("test", "classpath://filter.sql", jobConfiguration).start();
        Thread.sleep(60 * 1000);
    }

    @Test
    public void testCount() throws InterruptedException {
        sqlExecutionEnvironment.createJobFromPath("test", "classpath://count.sql").start();

        while (true) {
            Thread.sleep(1000);
        }
    }

    @Test
    public void testDim() throws InterruptedException {
        sqlExecutionEnvironment.createJobFromPath("test", "classpath://dim.sql").start();

        while (true) {
            Thread.sleep(1000);
        }
    }

    @Test
    public void testDimLeftJob() throws InterruptedException {
        sqlExecutionEnvironment.createJobFromPath("test", "classpath://dim_left_join.sql").start();

        while (true) {
            Thread.sleep(1000);
        }
    }

    @Test
    public void testJoin() throws InterruptedException {
        executeJoinJob("classpath://join.sql");
    }

    @Test
    public void testLeftJoin() throws InterruptedException {
        executeJoinJob("classpath://left_join.sql");
    }

    @Test
    public void testMergeSQL() throws InterruptedException {

        MergeSQLJobGraph mainJobGraph = SQLExecutionEnvironment.getExecutionEnvironment().createMergeSQLJobFromPath("test", "classpath://view.sql");
        mainJobGraph.start();
        JobGraph jobGraph = mainJobGraph.createJobFromPath("classpath://view_sub_sql_2.sql");
        jobGraph.start();
        mainJobGraph.createJobFromPath("classpath://view_sub_sql_1.sql").start();

        int count = 0;
        while (true) {
            Thread.sleep(1000);
            //            count++;
            //            if (count > 5) {
            //                System.out.println("destroy sql");
            //                jobGraph.stop();
            //            }
        }
    }

    @Test
    public void testDispatcherSQL() throws InterruptedException {

        DefaultDispatcherCallback defaultDispatcherCallback = new DefaultDispatcherCallback();

        SQLExecutionEnvironment executionEnvironment = SQLExecutionEnvironment.getExecutionEnvironment();
        DispatcherJobGraph dispatcherJobGraph = executionEnvironment.createDispatcherJob("test", defaultDispatcherCallback);
        dispatcherJobGraph.start();

        int i = 0;
        while (true) {
            i++;
            Thread.sleep(1000);
            if (i % 60 == 0) {
                //JobGraph jobGraph = executionEnvironment.createJobFromPath("test", "test_sql_" + i, "classpath://dispather_sub_sql.sql");
                //defaultDispatcherCallback.add("test_sql_" + i, jobGraph);
            }
        }
    }

    protected void executeJoinJob(String sql) throws InterruptedException {
        List<ChainPipeline<?>> pipelines = sqlExecutionEnvironment.createJobFromPath("test", sql).getPipelines();

        for (ChainPipeline<?> chainPipeline : pipelines) {
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    chainPipeline.startJob();
                }
            });
            thread.start();

        }

        while (true) {
            Thread.sleep(1000);
        }
    }
}
