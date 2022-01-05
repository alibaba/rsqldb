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
package com.alibaba.rsqldb.parser.builder;

import com.alibaba.rsqldb.parser.entity.SqlTask;
import org.apache.rocketmq.streams.configurable.ConfigurableComponent;

import java.util.Map;

/**
 * 同源,相同数据源的sql会动态装配在一起 可以通过getInsertSql 插入数据库完成任务发布，会自动和正在运行的数据源完成装配
 */
public class HomologousSQLBuilder extends SqlTask {
    protected String sourcePipelineName;//如果单表，单个数据源设置这个参数
    /**
     * 如果多个表，多个数据源，设置这个参数。 key是sql中source的表名，value是已经创建的数据源pipline的name
     */
    protected Map<String, String> sourcePipelineNames;

    public HomologousSQLBuilder(String namespace, String pipelineName, String sql, String sourcePipelineName) {
        super(namespace, pipelineName, sql);
        this.sourcePipelineName=sourcePipelineName;

    }

    public HomologousSQLBuilder(String namespace, String pipelineName, String sql, Map<String, String> sourceNames) {
        super(namespace, pipelineName, sql);
        this.sourcePipelineNames=sourceNames;

    }

    @Override
    protected SQLTreeBuilder createSQLTreeBuilder(
        ConfigurableComponent component) {
        if (sourcePipelineNames != null && sourcePipelineNames.size() > 0) {
            return new SQLTreeBuilder(namespace, pipelineName, sql, true, null, sourcePipelineNames);
        }
        return new SQLTreeBuilder(namespace, pipelineName, sql, true, sourcePipelineName, null);
    }

}
