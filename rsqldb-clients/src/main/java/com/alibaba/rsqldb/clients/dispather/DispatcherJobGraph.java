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
package com.alibaba.rsqldb.clients.dispather;

import java.util.List;
import java.util.Properties;

import com.alibaba.fastjson.JSONObject;

import org.apache.rocketmq.streams.common.configuration.ConfigurationKey;
import org.apache.rocketmq.streams.common.topology.IJobGraph;
import org.apache.rocketmq.streams.common.utils.IdUtil;
import org.apache.rocketmq.streams.dispatcher.IDispatcher;
import org.apache.rocketmq.streams.dispatcher.IDispatcherCallback;
import org.apache.rocketmq.streams.dispatcher.cache.DBCache;
import org.apache.rocketmq.streams.dispatcher.enums.DispatchMode;
import org.apache.rocketmq.streams.dispatcher.impl.LeaseDispatcher;

public class DispatcherJobGraph implements IJobGraph {
    public static String DISPATCHER_SCHEDULE_TIME = "dispatcher.schedule.time";
    public static String DISPATCHER_SCHEDULE_MODE = "dipper.dispatcher.mode";
    private final String url;
    private final String password;
    private final String userName;
    private final String dbDriver;
    private final String namespace;
    private final IDispatcherCallback<?> dispatcherCallback;
    private final Properties jobConfiguration;
    private int scheduleTime = 60;
    private String dispatchMode;
    private IDispatcher<?> dispatcher;

    public DispatcherJobGraph(String namespace, Properties jobConfiguration, IDispatcherCallback<?> dispatcherCallback) {
        this.namespace = namespace;
        if (namespace == null) {
            throw new RuntimeException("lost dispatcherGroup");
        }
        try {
            this.dbDriver = jobConfiguration.getProperty(ConfigurationKey.JDBC_DRIVER);
            this.url = jobConfiguration.getProperty(ConfigurationKey.JDBC_URL);
            this.userName = jobConfiguration.getProperty(ConfigurationKey.JDBC_USERNAME);
            this.password = jobConfiguration.getProperty(ConfigurationKey.JDBC_PASSWORD);
            Class.forName(dbDriver);
            if (this.url == null || this.userName == null || this.password == null) {
                throw new RuntimeException("lost lease db info " + this.namespace);
            }
            if (jobConfiguration.getProperty(DISPATCHER_SCHEDULE_TIME) != null) {
                this.scheduleTime = Integer.parseInt(jobConfiguration.getProperty(DISPATCHER_SCHEDULE_TIME));
            }

            this.dispatchMode = jobConfiguration.getProperty(DISPATCHER_SCHEDULE_MODE);
            this.dispatcherCallback = dispatcherCallback;
            this.jobConfiguration = jobConfiguration;
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void start() {
        if (this.dispatchMode == null) {
            this.dispatchMode = DispatchMode.LEAST.getName().toUpperCase();
        }
        try {
            if (this.dispatcher == null) {
                //根据不同的调度系统，构建不同的Dispatcher
                this.dispatcher = new LeaseDispatcher<>(this.dbDriver, this.url, this.userName, this.password, IdUtil.workerId(), this.namespace, DispatchMode.valueOf(this.dispatchMode), this.scheduleTime, this.dispatcherCallback, new DBCache(this.jobConfiguration));
                this.dispatcher.start();
            }

        } catch (Exception e) {
            throw new RuntimeException("start dispatcher job error", e);
        }
    }

    @Override
    public void stop() {
        try {
            this.dispatcher.close();
            this.dispatcherCallback.destroy();
        } catch (Exception e) {
            throw new RuntimeException("stop dispatcher job error", e);
        }
    }

    @Override
    public List<JSONObject> execute(List<JSONObject> dataList) {
        throw new RuntimeException("can not support this method");
    }

}
