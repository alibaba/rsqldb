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
package com.alibaba.rsqldb.parser.sql;

import java.util.List;
import org.apache.rocketmq.streams.configurable.ConfigurableComponent;
import org.apache.rocketmq.streams.serviceloader.ServiceLoaderComponent;

public class SqlParserProvider {

    public static ISqlParser create(String namespace, String name, String sql) {
        return create(namespace, name, sql, null);
    }

    /**
     * Find provider from jars
     */
    public static ISqlParser create(String namespace, String name, String sql, ConfigurableComponent component) {
        ServiceLoaderComponent<IParserProvider> serviceLoaderComponent = (ServiceLoaderComponent<IParserProvider>) ServiceLoaderComponent.getInstance(IParserProvider.class);
        List<IParserProvider> parserProviders = serviceLoaderComponent.loadService();
        if (parserProviders == null) {
            throw new RuntimeException("can not find SQLParserProvider interface's impl, please check if exist");
        }
        IParserProvider parserProvider = parserProviders.get(0);
        return parserProvider.createSqlParser(namespace, name, sql, component);
    }
}
