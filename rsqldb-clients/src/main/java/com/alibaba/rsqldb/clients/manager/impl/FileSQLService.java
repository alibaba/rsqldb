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
package com.alibaba.rsqldb.clients.manager.impl;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.alibaba.rsqldb.clients.SQLExecutionEnvironment;
import com.alibaba.rsqldb.clients.manager.ISQLService;
import com.alibaba.rsqldb.clients.manager.SQLForJob;

import com.google.auto.service.AutoService;
import org.apache.rocketmq.streams.common.channel.impl.view.ViewSource;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.model.ServiceName;
import org.apache.rocketmq.streams.common.topology.graph.JobGraph;
import org.apache.rocketmq.streams.common.utils.FileUtil;

@AutoService(ISQLService.class)
@ServiceName("FILE")
public class FileSQLService implements ISQLService {
    public static String BASED_DIR = "basedDir";

    protected String basedDir;

    public FileSQLService() {
    }

    public FileSQLService(String basedDir) {
        if (basedDir.startsWith(FileUtil.CLASS_PATH_FILE_HEADER)) {
            basedDir = FileUtil.getResourceFile(basedDir.replace(FileUtil.CLASS_PATH_FILE_HEADER, "")).getAbsolutePath();
        }
        this.basedDir = basedDir;
    }

    @Override
    public void initFromProperties(Properties properties) {
        String basedDir = properties.getProperty(BASED_DIR);

        if (basedDir != null) {
            if (basedDir.startsWith(FileUtil.CLASS_PATH_FILE_HEADER)) {
                basedDir = FileUtil.getResourceFile(basedDir.replace(FileUtil.CLASS_PATH_FILE_HEADER, "")).getAbsolutePath();
            }
            this.basedDir = basedDir;
        }
    }

    @Override
    public List<SQLForJob> select(String namespace) {
        String dirPath = FileUtil.concatDir(basedDir, namespace);
        File dir = new File(dirPath);
        if (dir.isDirectory() && dir.exists()) {
            File[] files = dir.listFiles();
            if (files != null) {
                List<SQLForJob> sqlForJobs = new ArrayList<>();
                for (File file : files) {
                    SQLForJob sqlForJob = new SQLForJob();
                    sqlForJob.setNameSpace(namespace);
                    sqlForJob.setName(file.getName());
                    sqlForJob.setVersion(file.lastModified());
                    sqlForJob.setSql(FileUtil.loadFileContentContainLineSign(file.getAbsolutePath()));
                    JobGraph jobGraph = SQLExecutionEnvironment.getExecutionEnvironment().createJob(sqlForJob.getNameSpace(), sqlForJob.getName(), sqlForJob.getSql());
                    if (jobGraph.getPipelines().size() == 1) {
                        ISource source = jobGraph.getPipelines().get(0).getSource();
                        if (source instanceof ViewSource) {
                            sqlForJob.setParentSQLName(((ViewSource)source).getTableName());
                        }
                    }
                    sqlForJobs.add(sqlForJob);
                }
                return sqlForJobs;
            }

        }
        return null;
    }

    @Override
    public SQLForJob select(String namespace, String name) {
        List<SQLForJob> sqlForJobs = select(namespace);
        if (sqlForJobs == null) {
            return null;
        }
        for (SQLForJob sqlForJob : sqlForJobs) {
            if (sqlForJob.getName().equals(name)) {
                return sqlForJob;
            }
        }
        return null;
    }

    @Override
    public void saveOrUpdate(SQLForJob sqlForJob) {
        String dirPath = FileUtil.concatDir(basedDir, sqlForJob.getNameSpace());
        File dir = new File(dirPath);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        FileUtil.write(dir.getAbsoluteFile() + File.separator + sqlForJob.getName(), sqlForJob.getSql(), true);
    }

    @Override
    public void delete(String namespace, String name) {
        String dirPath = FileUtil.concatDir(basedDir, namespace);
        File file = new File(dirPath + File.separator + name);
        file.delete();
    }

    public String getBasedDir() {
        return basedDir;
    }

}
