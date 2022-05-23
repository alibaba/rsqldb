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

import java.util.Properties;

public abstract class AbstractStream<T> {

    protected Properties properties = new Properties();

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

    public T dbConfig(String dbType, String dbUrl, String dbUsername, String dbPassword) {
        return dbConfig(dbType, dbUrl, dbUsername, dbPassword, "dipper_configure");
    }

    public T dbConfig(String dbType, String dbUrl, String dbUsername, String dbPassword, String dbTableName) {
        return dbConfig(dbType, dbUrl, dbUsername, dbPassword, dbTableName, "com.mysql.jdbc.Driver");
    }

    public T dbConfig(String dbType, String dbUrl, String dbUsername, String dbPassword, String dbTableName, String dbDriver) {
        this.properties.setProperty("dipper.rds.jdbc.type", dbType);
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

    public T memoryConfig() {
        return (T) this;
    }
}
