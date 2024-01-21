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

import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;

public class SQLForJob extends BasedConfigurable {
    public static String TYPE = "sql";

    protected String sql;
    protected long version;//每次sql变化时更新
    protected String parentSQLName;//在view场景使用，一般情况不需要配置
    protected int weight = 1;//分配的权重，在dispather场景使用

    public SQLForJob() {
        setType(TYPE);
    }

    public static String getTYPE() {
        return TYPE;
    }

    public static void setTYPE(String TYPE) {
        SQLForJob.TYPE = TYPE;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public String getParentSQLName() {
        return parentSQLName;
    }

    public void setParentSQLName(String parentSQLName) {
        this.parentSQLName = parentSQLName;
    }

    public int getWeight() {
        return weight;
    }

    public void setWeight(int weight) {
        this.weight = weight;
    }
}
