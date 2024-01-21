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
import java.util.ArrayList;
import java.util.List;

import com.alibaba.rsqldb.clients.merge.MergeSQLJobGraph;

import org.apache.rocketmq.streams.common.configuration.JobConfiguration;
import org.apache.rocketmq.streams.common.topology.graph.JobGraph;
import org.apache.rocketmq.streams.common.utils.FileUtil;
import org.junit.Test;

public class IOTTest {
    @Test
    public void testFunctionValueChangeMQTT() throws InterruptedException {
        String namespace = "test";
        JobGraph mainJobGraph = SQLExecutionEnvironment.getExecutionEnvironment().createJobFromPath(namespace, "classpath://iot_test.sql");
        mainJobGraph.start();

        //        JobGraph jobGraph1 = mainJobGraph.createJobFromPath("classpath://iot_sub_sql/3400.sql");
        //        jobGraph1.start();
        //        System.out.println("start job 1");
        //
        //        JobGraph jobGraph2 = mainJobGraph.createJobFromPath("classpath://iot_sub_sql/140.sql");
        //        jobGraph2.start();
        //        System.out.println("start job 2");

        int count = 0;
        boolean isStart = true;
        while (true) {
            Thread.sleep(1000);
            //            count++;
            //            if (count % 60 == 0 && count >= 60) {
            //                if (isStart) {
            //                    jobGraph2.stop();
            //                    ;
            //                    System.out.println("stop job 2");
            //                    isStart = false;
            //                } else {
            //                    jobGraph2.start();
            //                    System.out.println("start job 2");
            //                    isStart = true;
            //                }
            //            }
        }
    }

    @Test
    public void testAll() throws InterruptedException {
        JobConfiguration jobConfiguration = new JobConfiguration().openHtmlMonitor();

        String namespace = "test";
        MergeSQLJobGraph mainJobGraph = SQLExecutionEnvironment.getExecutionEnvironment().createMergeSQLJobFromPath(namespace, "classpath://iot_source_R.sql", jobConfiguration);
        mainJobGraph.start();

        List<JobGraph> subJobGraphs1 = new ArrayList<>();
        File file = FileUtil.getResourceFile("iot_sub_sql1").getAbsoluteFile();
        for (File subFile : file.listFiles()) {
            JobGraph jobGraph = mainJobGraph.createJobFromPath("classpath://iot_sub_sql1/" + subFile.getName());
            subJobGraphs1.add(jobGraph);
            jobGraph.start();
        }
        System.out.println("start subJobGraphs1");
        List<JobGraph> subJobGraphs2 = new ArrayList<>();
        file = FileUtil.getResourceFile("iot_sub_sql2").getAbsoluteFile();
        for (File subFile : file.listFiles()) {
            JobGraph jobGraph = mainJobGraph.createJobFromPath("classpath://iot_sub_sql2/" + subFile.getName());
            subJobGraphs2.add(jobGraph);
            jobGraph.start();
        }
        System.out.println("start subJobGraphs2");
        System.out.println("finish iot sql load");

        int count = 0;
        boolean isStart = true;
        while (true) {
            Thread.sleep(1000);
            count++;
            if (count % 60 == 0 && count >= 60) {
                if (isStart) {
                    for (JobGraph jobGraph : subJobGraphs2) {
                        jobGraph.stop();
                    }
                    System.out.println("stop subJobGraphs2");
                    isStart = false;
                } else {
                    for (JobGraph jobGraph : subJobGraphs2) {
                        jobGraph.start();
                    }
                    System.out.println("start subJobGraphs2");
                    isStart = true;
                }
            }
        }
    }

}
