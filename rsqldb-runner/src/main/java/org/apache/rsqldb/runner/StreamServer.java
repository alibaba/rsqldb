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

package org.apache.rsqldb.runner;

import com.beust.jcommander.internal.Lists;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.streams.RocketMQChannelBuilder;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configure.ConfigureFileKey;
import org.apache.rocketmq.streams.common.topology.task.StreamsTask;
import org.apache.rocketmq.streams.common.utils.PropertiesUtils;
import org.apache.rocketmq.streams.configurable.ConfigurableComponent;

public class StreamServer {
    private static ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(5);

    public static void main(String[] args) {
        String namespace = "default";
        if (args.length > 0) {
            namespace = args[0];
        }

        //获取mqnamesrv文件信息
        List<String> mqNameServers = Lists.newArrayList();
        InputStream inputStream = null;
        String line;
        try {
            inputStream = StreamServer.class.getClassLoader().getResourceAsStream("mqnamesrv");
            if (inputStream != null) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                while ((line = reader.readLine()) != null) {
                    mqNameServers.add(line.trim() + ":9876");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (inputStream != null) {
                    inputStream.close();
                }
            } catch (Exception e1) {
                e1.printStackTrace();
            }
        }
        Properties properties = PropertiesUtils.getResourceProperties("dipper.properties");
        String shuffleChannelType = properties.getProperty("window.shuffle.channel.type");
        if (RocketMQChannelBuilder.TYPE.equals(shuffleChannelType) && !mqNameServers.isEmpty()) {
            properties.setProperty("window.shuffle.channel.namesrvAddr", String.join(";", mqNameServers));
            properties.setProperty("window.shuffle.channel.topic", "default_shuffle_topic");
            properties.setProperty("window.shuffle.channel.group", "default_shuffle_group");
        }
        ComponentCreator.setProperties(properties);
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
