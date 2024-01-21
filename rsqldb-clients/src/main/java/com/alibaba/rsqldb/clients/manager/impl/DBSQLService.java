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

import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import com.alibaba.rsqldb.clients.manager.ISQLService;
import com.alibaba.rsqldb.clients.manager.SQLForJob;

import org.apache.rocketmq.streams.common.configuration.ConfigurationKey;
import org.apache.rocketmq.streams.common.configuration.SystemContext;
import org.apache.rocketmq.streams.db.driver.orm.ORMUtil;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

public class DBSQLService implements ISQLService {
    private String tableName = "dipper_sql";
    private String url;
    private String userName;
    private String password;

    public DBSQLService(String url, String userName, String password) {
        this.url = url;
        this.userName = userName;
        this.password = password;
    }

    public DBSQLService() {
        this.url = SystemContext.getProperty(ConfigurationKey.JDBC_URL);
        this.userName = SystemContext.getProperty(ConfigurationKey.JDBC_USERNAME);
        this.password = SystemContext.getProperty(ConfigurationKey.JDBC_PASSWORD);

    }

    @Override
    public void initFromProperties(Properties properties) {
        if (properties.getProperty("url") != null) {
            this.url = properties.getProperty("url");
        }
        if (properties.getProperty("userName") != null) {
            this.userName = properties.getProperty("userName");
        }
        if (properties.getProperty("password") != null) {
            this.password = properties.getProperty("password");
        }
    }

    @Override
    public List<SQLForJob> select(String namespace) {
        if (FunctionUtils.isConstant(namespace)) {
            throw new RuntimeException(namespace + " can not contains ', may be sql insect");
        }
        return ORMUtil.queryForList("select * from `" + this.tableName + "` where namespace='" + namespace + "'", new HashMap<>(), SQLForJob.class, url, userName, password);
    }

    @Override
    public SQLForJob select(String namespace, String name) {

        if (FunctionUtils.isConstant(namespace) || FunctionUtils.isConstant(name)) {
            throw new RuntimeException(namespace + " can not contains ', may be sql insect");
        }
        return ORMUtil.queryForObject("select * from `" + this.tableName + "` where namespace='" + namespace + "' and name='" + name + "'", new HashMap<>(), SQLForJob.class, url, userName, password);
    }

    @Override
    public void saveOrUpdate(SQLForJob sqlForJob) {
        if (sqlForJob != null) {
            ORMUtil.batchReplaceInto(sqlForJob);
        }

    }

    @Override
    public void delete(String namespace, String name) {
        ORMUtil.executeSQL(url, userName, password, "delete from `" + this.tableName + "` where namespace='" + namespace + "' and name='" + name + "'", new HashMap<>());
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
