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
package com.alibaba.rsqldb.clients.manager;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.alibaba.rsqldb.clients.SQLExecutionEnvironment;
import com.alibaba.rsqldb.clients.manager.impl.DBSQLService;
import com.alibaba.rsqldb.clients.manager.impl.FileSQLService;

import org.apache.rocketmq.streams.common.channel.impl.view.ViewSource;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.configuration.ConfigurationKey;
import org.apache.rocketmq.streams.common.configuration.SystemContext;
import org.apache.rocketmq.streams.common.threadpool.ScheduleFactory;
import org.apache.rocketmq.streams.common.topology.graph.JobGraph;
import org.apache.rocketmq.streams.common.utils.FileUtil;
import org.apache.rocketmq.streams.common.utils.ServiceLoadUtil;

public class SQLManager {
    protected ISQLService sqlService;
    protected String namespace;

    private SQLManager(String namespace) {
        this.namespace = namespace;
    }

    public static SQLManager createFromProperties(String namespace) {
        String storageType = SystemContext.getProperty(ConfigurationKey.SQL_STORAGE_TYPE);
        if (storageType == null) {
            storageType = SQLStrogeEnum.MEMORY.getName();
        }
        SQLManager sqlManager = createSQLManager(namespace, storageType);
        sqlManager.sqlService.initFromProperties(SystemContext.getProperties());
        return sqlManager;
    }

    public static SQLManager createFromMemory(String namespace) {
        String storageType = SQLStrogeEnum.MEMORY.getName();
        return createSQLManager(namespace, storageType);
    }

    public static SQLManager createFromDB(String namespace) {
        SQLManager sqlManager = new SQLManager(namespace);
        sqlManager.sqlService = new DBSQLService();
        return sqlManager;
    }

    public static SQLManager createFromDB(String namespace, String url, String userName, String password) {
        SQLManager sqlManager = new SQLManager(namespace);
        sqlManager.sqlService = new DBSQLService(url, userName, password);
        return sqlManager;
    }

    public static SQLManager createFromDir(String namespace, String basedDir) {
        SQLManager sqlManager = new SQLManager(namespace);
        sqlManager.sqlService = new FileSQLService(basedDir);
        return sqlManager;
    }

    private static SQLManager createSQLManager(String namespace, String storageType) {

        ISQLService sqlService = ServiceLoadUtil.loadService(ISQLService.class, storageType);
        if (sqlService == null) {
            throw new RuntimeException("can not found ISQLService'impl, please check had import jar");
        }
        SQLManager sqlManager = new SQLManager(namespace);
        sqlManager.sqlService = sqlService;
        return sqlManager;
    }

    public List<SQLForJob> select() {
        return sqlService.select(namespace);
    }

    public List<SQLForJob> selectByParentSQLName(String parentSQLName) {
        List<SQLForJob> list = sqlService.select(namespace);
        if (list == null) {
            return null;
        }
        List<SQLForJob> result = new ArrayList<>();
        for (SQLForJob sqlForJob : list) {
            if (sqlForJob.getParentSQLName() != null && sqlForJob.getParentSQLName().equals(parentSQLName)) {
                result.add(sqlForJob);
            }
        }
        return result;

    }

    public SQLForJob select(String sqlName) {
        return sqlService.select(namespace, sqlName);
    }

    public void saveOrUpdate(SQLForJob sqlForJob) {
        sqlService.saveOrUpdate(sqlForJob);
    }

    public void saveOrUpdate(String sqlPath) {
        SQLForJob sqlForJob = new SQLForJob();
        sqlForJob.setNameSpace(namespace);
        sqlForJob.setSql(FileUtil.loadFileContentContainLineSign(sqlPath));
        String sqlName = createSQLNameFromSqlPath(sqlPath);
        sqlForJob.setName(sqlName);
        JobGraph jobGraph = SQLExecutionEnvironment.getExecutionEnvironment().createJob(namespace, sqlName,
            sqlForJob.getSql());
        if (jobGraph.getPipelines().size() == 1) {
            ISource<?> source = jobGraph.getPipelines().get(0).getSource();
            if (source instanceof ViewSource) {
                ViewSource viewSource = (ViewSource)source;
                sqlForJob.setParentSQLName(viewSource.getTableName());
            }
        }
        saveOrUpdate(sqlForJob);
    }

    public void delete(String sqlName) {
        sqlService.delete(namespace, createSQLNameFromSqlPath(sqlName));
    }

    public void startSQLChangedListen(String namespace, String parentSQLName, int initialDelaySecond, int delaySecond, ISQLChangedListener listener) {
        ScheduleFactory.getInstance().execute(namespace + "-" + parentSQLName + "-sql_channel_listener_schedule", () -> {

            if (listener != null) {
                List<SQLForJob> sqlForJobs = sqlService.select(namespace);
                if (sqlForJobs == null) {
                    return;
                }
                List<SQLForJob> result = new ArrayList<>();
                for (SQLForJob sqlForJob : sqlForJobs) {
                    if (parentSQLName.equals(sqlForJob.getParentSQLName())) {
                        result.add(sqlForJob);
                    }
                }
                listener.notifySQLForJobChanged(result);
            }

        }, initialDelaySecond, delaySecond, TimeUnit.SECONDS);
    }

    public void stopSQLChangedListen(String namespace, String parentSQLName) {
        ScheduleFactory.getInstance().cancel(namespace + "-" + parentSQLName + "-sql_channel_listener_schedule");
    }

    private String createSQLNameFromSqlPath(String sqlPath) {
        String sqlName = sqlPath;
        if (sqlPath.indexOf(File.separator) != -1) {
            int lastIndex = sqlPath.lastIndexOf(File.separator);
            if (lastIndex != -1) {
                sqlName = sqlPath.substring(lastIndex + 1, sqlPath.length()).replace(".", "_");
            }
        }
        return sqlName;
    }
}
