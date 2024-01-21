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
package com.alibaba.rsqldb.parser.connector.source;

import java.util.HashMap;
import java.util.Map;

import com.alibaba.rsqldb.parser.connector.DipperSourceContext;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.rocketmq.streams.common.channel.source.AbstractSingleSplitSource;

public class FlinkSingleSplitSource extends AbstractSingleSplitSource {
    private static Map<String, DynamicTableSourceFactory> sourceFactoryMap = new HashMap<>();
    protected String connector;
    protected transient SourceFunction<RowData> sourceFunction;

    public static void registe(DynamicTableSourceFactory factory) {
        sourceFactoryMap.put(factory.factoryIdentifier(), factory);
    }

    @Override
    protected boolean initConfigurable() {
        sourceFunction = createSourceFunction();
        return super.initConfigurable();
    }

    @Override
    protected boolean startSource() {
        try {
            sourceFunction.run(new DipperSourceContext(this));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return true;
    }

    @Override
    protected void destroySource() {
        sourceFunction.cancel();
    }

    protected SourceFunction<RowData> createSourceFunction() {
        DynamicTableSourceFactory dynamicTableSourceFactory = sourceFactoryMap.get(connector);
        if (dynamicTableSourceFactory == null) {
            throw new RuntimeException("create flink " + connector + " connector error ");
        }
        return null;
    }

    protected DynamicTableFactory.Context createContext() {
        return new DynamicTableFactory.Context() {

            @Override
            public ObjectIdentifier getObjectIdentifier() {
                return null;
            }

            @Override
            public ResolvedCatalogTable getCatalogTable() {
                return null;
            }

            @Override
            public ReadableConfig getConfiguration() {
                return null;
            }

            @Override
            public ClassLoader getClassLoader() {
                return null;
            }

            @Override
            public boolean isTemporary() {
                return false;
            }
        };
    }

    public String getConnector() {
        return connector;
    }

    public void setConnector(String connector) {
        this.connector = connector;
    }
}
