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
package com.alibaba.rsqldb.clients.sql;

import com.alibaba.rsqldb.parser.sql.ISqlParser;
import com.alibaba.rsqldb.parser.sql.SqlParserProvider;
import java.util.List;
import java.util.Properties;
import org.apache.rocketmq.streams.common.component.AbstractComponent;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.configurable.ConfigurableComponent;
import org.apache.rocketmq.streams.tasks.StreamTask;

public abstract class AbstractStream<T> {

    protected String namespace;
    protected ConfigurableComponent configurableComponent;

    protected Properties properties = new Properties();

    public AbstractStream(String namespace) {
        this.namespace = namespace;
    }

    public T init() {
        return init(true);
    }

    public T init(boolean isStart) {
        ComponentCreator.updateProperties(this.properties);
        if (isStart) {
            this.configurableComponent = ComponentCreator.getComponent(this.namespace, ConfigurableComponent.class);
        } else {
            this.configurableComponent = ComponentCreator.getComponentNotStart(this.namespace, ConfigurableComponent.class);
        }
        return (T) this;
    }

    public AbstractStream() {
        properties.put(AbstractComponent.POLLING_TIME, "10");
    }

    public T db() {
        this.properties.setProperty("dipper.configurable.service.type", "DB");
        return (T) this;
    }

    public T file() {
        this.properties.setProperty("dipper.configurable.service.type", "file");
        return (T) this;
    }

    public T memory() {
        this.properties.setProperty("dipper.configurable.service.type", "memory");
        return (T) this;
    }

    public T dbConfig(String dbUrl, String dbUsername, String dbPassword) {
        return dbConfig(dbUrl, dbUsername, dbPassword, "dipper_configure");
    }

    public T dbConfig(String dbUrl, String dbUsername, String dbPassword, String dbTableName) {
        return dbConfig(dbUrl, dbUsername, dbPassword, dbTableName, "com.mysql.jdbc.Driver");
    }

    public T dbConfig(String dbUrl, String dbUsername, String dbPassword, String dbTableName, String dbDriver) {
        this.properties.setProperty("dipper.rds.jdbc.url", dbUrl);
        this.properties.setProperty("dipper.rds.jdbc.username", dbUsername);
        this.properties.setProperty("dipper.rds.jdbc.password", dbPassword);
        this.properties.setProperty("dipper.rds.table.name", dbTableName);
        this.properties.setProperty("dipper.rds.jdbc.driver", dbDriver);
        return (T) this;
    }

    public T fileConfig() {
        return fileConfig("dipper.cs");
    }

    public T fileConfig(String filePathAndName) {
        this.properties.setProperty("filePathAndName", filePathAndName);
        return (T) this;
    }

    protected ISqlParser createSqlTreeBuilder(String namespace, String name, String sql, ConfigurableComponent component) {
        return SqlParserProvider.create(namespace, name, sql, component);
    }

    public T memoryConfig() {
        return (T) this;
    }

    void upsertConfigurable(List<IConfigurable> configurables) {
        for (IConfigurable iConfigurable : configurables) {
            BasedConfigurable oldConfigurable = (BasedConfigurable) this.configurableComponent.queryConfigurableByIdent(iConfigurable.getType(), iConfigurable.getConfigureName());
            if (oldConfigurable == null) {
                ((BasedConfigurable) iConfigurable).setUpdateFlag(0);
            } else {
                ((BasedConfigurable) iConfigurable).setUpdateFlag(oldConfigurable.getUpdateFlag() + 1);
                ((BasedConfigurable) iConfigurable).setState(oldConfigurable.getState());
            }
            this.configurableComponent.insertToCache(iConfigurable);
        }
        this.configurableComponent.flushCache();
    }

    /**
     * 任务启动
     *
     * @throws Exception exception
     */
    abstract void start() throws Exception;

    /**
     * 任务停止
     *
     * @throws Exception exception
     */
    abstract void stop() throws Exception;

    /**
     * 罗列所有任务
     *
     * @return 任务列表
     * @throws Exception exception
     */
    abstract List<StreamTask> list() throws Exception;

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }
}
