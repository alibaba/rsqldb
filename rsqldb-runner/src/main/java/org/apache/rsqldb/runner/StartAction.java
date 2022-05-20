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

import com.alibaba.rsqldb.parser.entity.SqlTask;
import com.google.common.collect.Lists;
import org.apache.rocketmq.streams.common.utils.FileUtil;
import org.apache.rocketmq.streams.common.utils.PropertiesUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.io.File;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * @author junjie.cheng
 * @date 2021/11/19
 */
public class StartAction {

    private static final Logger LOGGER = LoggerFactory.getLogger(StartAction.class);

    public static void main(String[] args) {
        if (args.length < 1) {
            return;
        }
        String jobPath = args[0];
        String namespace = null;
        if (args.length > 1) {
            namespace = args[1];
        }
        String jobName = null;
        if (args.length > 2) {
            jobName = args[2];
        }
        List<SqlTask> allSqlTasks = parseTask(jobPath);
        List<SqlTask> runningTasks = Lists.newArrayList();

        //如果namespace为空且jobName也为空， 则表示启动该目录下所有的任务
        if (namespace == null && jobName == null) {
            runningTasks = allSqlTasks;
            //如果namespace不为空但jobName为空， 则标识启动该namespace下所有的任务
        } else if (namespace != null && jobName == null) {
            String finalNamespace = namespace;
            runningTasks = allSqlTasks.stream().filter(runnerEntity -> finalNamespace.equals(runnerEntity.getNamespace())).collect(Collectors.toList());
            //如果namespace不为空且jobName不为空， 则标识启动该namespace下指定的name的任务
        } else if (namespace != null && jobName != null) {
            String finalNamespace = namespace;
            final List<?> jobNames = CollectionUtils.arrayToList(jobName.split(","));
            runningTasks = allSqlTasks.stream().filter(runnerEntity -> finalNamespace.equals(runnerEntity.getNamespace()) && jobNames.contains(runnerEntity.getPipelineName())).collect(Collectors.toList());
        }
        Properties properties1 = PropertiesUtils.getResourceProperties("dipper.properties");
        if (properties1.isEmpty()) {
            System.out.println("资源文件为空");
        } else {
            for (Entry<Object, Object> entry : properties1.entrySet()) {
                System.err.println(entry.getKey().toString() + ":" + entry.getValue().toString());
            }
        }

        for (SqlTask runningTask : runningTasks) {
            runningTask.startSQL();
        }

    }

    public static List<SqlTask> parseTask(String jobPath) {
        List<SqlTask> sqlTasks = Lists.newArrayList();
        File jobFile = new File(jobPath);
        File[] files = jobFile.listFiles();
        if (files != null && files.length > 0) {
            for (File file : files) {
                if (file.isDirectory()) {
                    String namespace = file.getName();
                    File[] sqlFiles = file.listFiles((dir, name) -> name.endsWith(".sql"));
                    if (sqlFiles != null && sqlFiles.length > 0) {
                        for (File sqlFile : sqlFiles) {
                            String fileName = sqlFile.getName();
                            String taskName = fileName.substring(0, fileName.indexOf("."));
                            String sql = FileUtil.loadFileContentContainLineSign(sqlFile.getAbsolutePath());
                            sqlTasks.add(new SqlTask(namespace, taskName, sql));
                        }
                    }
                } else if (file.getName().endsWith(".sql")) {
                    String fileName = file.getName();
                    String namespace = fileName.substring(0, fileName.indexOf("."));
                    String taskName = fileName.substring(0, fileName.indexOf("."));
                    String sql = FileUtil.loadFileContentContainLineSign(file.getAbsolutePath());
                    sqlTasks.add(new SqlTask(namespace, taskName, sql));
                }
            }
        }
        return sqlTasks;
    }

}
