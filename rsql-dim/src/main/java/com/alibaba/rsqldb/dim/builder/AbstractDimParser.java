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
package com.alibaba.rsqldb.dim.builder;

import com.alibaba.rsqldb.dim.model.AbstractDim;
import java.util.Properties;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.utils.StringUtil;

public abstract class AbstractDimParser implements IDimSQLParser {
    @Override
    public AbstractDim parseDim(String namespace, Properties properties, MetaData metaData) {
        AbstractDim dim = createDim(properties, metaData);
        String cacheTTLMs = properties.getProperty("cacheTTLMs");
        long pollingTime = 30;//默认更新时间是30分钟

        if (StringUtil.isNotEmpty(cacheTTLMs)) {
            pollingTime = (Long.valueOf(cacheTTLMs) / 1000 / 60);
        }
        dim.setNameSpace(namespace);
        dim.setPollingTimeMinute(pollingTime);

        String isLarge = properties.getProperty("isLarge");
        if (isLarge == null || "false".equalsIgnoreCase(isLarge)) {
            return dim;
        }
        dim.setLarge(Boolean.valueOf(isLarge));
        String filePath = properties.getProperty("filePath");
//        String tableName = metaData.getTableName();
        if (filePath == null) {
            throw new RuntimeException("if large table is true, must set file path args");
        }
        dim.setFilePath(filePath);
        return dim;
    }

    protected abstract AbstractDim createDim(Properties properties, MetaData data);
}
