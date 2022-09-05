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

package org.alibaba.rsqldb.runner;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.streams.RocketMQChannelBuilder;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configure.ConfigureFileKey;
import org.apache.rocketmq.streams.common.topology.task.StreamsTask;
import org.apache.rocketmq.streams.configurable.ConfigurableComponent;
import org.apache.rocketmq.streams.script.function.service.impl.ScanFunctionService;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class StreamRunner {
    private static ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(5);

    public static void main(String[] args) throws Throwable {
        if (args == null || args.length < 1) {
            throw new IllegalArgumentException("home.dir is required.");
        }

        String homeDir = args[0];

        String configFile = homeDir + "/conf/rsqldb.conf";

        InputStream in = new BufferedInputStream(new FileInputStream(configFile));
        Properties properties = new Properties();
        properties.load(in);

        Map<String, String> env = System.getenv();
        String namesrvAddrEnv = env.get(MixAll.NAMESRV_ADDR_ENV);
        if (namesrvAddrEnv != null) {
            properties.put(MixAll.NAMESRV_ADDR_ENV, namesrvAddrEnv);
        }

        String filePathAndName = properties.getProperty("filePathAndName");
        if (StringUtils.isEmpty(filePathAndName)) {
            throw new IllegalArgumentException("filePathAndName is required.");
        }

        String namesrvAddrs = properties.getProperty(MixAll.NAMESRV_ADDR_ENV);

        String[] temp = namesrvAddrs.trim().split(";");
        List<String> mqNameServers = Arrays.asList(temp);

        String shuffleChannelType = properties.getProperty("window.shuffle.channel.type");
        if (RocketMQChannelBuilder.TYPE.equals(shuffleChannelType) && !mqNameServers.isEmpty()) {
            properties.setProperty("window.shuffle.channel.namesrvAddr", String.join(";", mqNameServers));
            properties.setProperty("window.shuffle.channel.topic", "default_shuffle_topic");
            properties.setProperty("window.shuffle.channel.group", "default_shuffle_group");
        }
        ComponentCreator.setProperties(properties);

        String namespace = properties.getProperty("namespace");
        if (StringUtils.isEmpty(namespace)) {
            namespace = "default";
        }

        //在维表join时需要使用leftJoin名函数，需要先加载
        ScanFunctionService.getInstance().scanePackage("com.alibaba.rsqldb.dim.function");
        ConfigurableComponent configurableComponent = ComponentCreator.getComponent(namespace, ConfigurableComponent.class);
        final String finalNamespace = namespace;

        long rollingTime = 60L;
        String rollingTimeStr = ComponentCreator.getProperties().getProperty(ConfigureFileKey.POLLING_TIME);
        if (rollingTimeStr != null && !rollingTimeStr.isEmpty()) {
            rollingTime = Long.parseLong(rollingTimeStr);
        }
        scheduledExecutorService.scheduleWithFixedDelay(() -> {
            configurableComponent.refreshConfigurable(finalNamespace);
            if (ComponentCreator.getProperties() == null) {
                System.out.println("资源文件为空");
            } else {
                Properties properties1 = ComponentCreator.getProperties();
                for (Object key : properties1.keySet()) {
                    System.out.println(key.toString() + "=" + properties1.get(key).toString());
                }
            }

            List<StreamsTask> streamsTasks = configurableComponent.queryConfigurableByType(StreamsTask.TYPE);
            for (StreamsTask streamsTask : streamsTasks) {
                if (StreamsTask.STATE_STARTED.equals(streamsTask.getState())) {
                    streamsTask.start();
                } else {
                    streamsTask.destroy();
                }
            }
        }, 0, rollingTime, TimeUnit.SECONDS);
    }

}
