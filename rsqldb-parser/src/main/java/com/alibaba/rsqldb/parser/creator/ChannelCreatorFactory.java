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
package com.alibaba.rsqldb.parser.creator;

import java.util.Properties;

import org.apache.rocketmq.streams.common.channel.builder.IChannelBuilder;
import org.apache.rocketmq.streams.common.channel.sink.AbstractSink;
import org.apache.rocketmq.streams.common.channel.sink.ISink;
import org.apache.rocketmq.streams.common.channel.source.AbstractSource;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.utils.ContantsUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.serviceloader.ServiceLoaderComponent;

public class ChannelCreatorFactory {

    public static ISource<?> createSource(String namespace, String name, Properties properties, MetaData metaData) {
        String type = properties.getProperty("type");
        if (StringUtil.isEmpty(type)) {
            type = properties.getProperty("TYPE");
        }
        if (StringUtil.isEmpty(type)) {
            type = properties.getProperty("connector");
        }
        if (StringUtil.isEmpty(type)) {
            type = properties.getProperty("CONNECTOR");
        }
        if (ContantsUtil.isContant(type)) {
            type = type.substring(1, type.length() - 1);
        }
        ServiceLoaderComponent<?> serviceLoaderComponent = ComponentCreator.getComponent(IChannelBuilder.class.getName(), ServiceLoaderComponent.class);
        IChannelBuilder builder = (IChannelBuilder)serviceLoaderComponent.loadService(type.toLowerCase());

        if (builder == null) {
            throw new RuntimeException("expect channel creator for " + properties.getProperty("type") + ". but not found");
        }
        ISource<?> source = builder.createSource(namespace, name, properties, metaData);
        if (source instanceof AbstractSource) {
            ((AbstractSource)source).setMetaData(metaData);
        }
        return source;
    }

    public static ISink<?> createSink(String namespace, String name, Properties properties, MetaData metaData) {
        String type = properties.getProperty("type");
        if (StringUtil.isEmpty(type)) {
            type = properties.getProperty("TYPE");
        }
        if (StringUtil.isEmpty(type)) {
            type = properties.getProperty("connector");
        }
        if (StringUtil.isEmpty(type)) {
            type = properties.getProperty("CONNECTOR");
        }
        if (ContantsUtil.isContant(type)) {
            type = type.substring(1, type.length() - 1);
        }
        ServiceLoaderComponent<?> serviceLoaderComponent = ComponentCreator.getComponent(IChannelBuilder.class.getName(), ServiceLoaderComponent.class);
        IChannelBuilder builder = (IChannelBuilder)serviceLoaderComponent.loadService(type.toLowerCase());

        if (builder == null) {
            throw new RuntimeException(
                "expect channel creator for " + properties.getProperty("type") + ". but not found");
        }
        ISink sink= builder.createSink(namespace, name, properties, metaData);
        if (sink instanceof AbstractSink) {
            ((AbstractSink)sink).setMetaData(metaData);
        }
        return sink;
    }

}
