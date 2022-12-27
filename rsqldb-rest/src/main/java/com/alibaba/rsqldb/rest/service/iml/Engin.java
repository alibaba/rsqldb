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
package com.alibaba.rsqldb.rest.service.iml;

import com.alibaba.rsqldb.common.exception.RSQLServerException;
import com.alibaba.rsqldb.parser.impl.BuildContext;
import com.alibaba.rsqldb.parser.model.Node;
import com.alibaba.rsqldb.parser.model.statement.Statement;
import com.alibaba.rsqldb.rest.service.RSQLConfig;
import com.alibaba.rsqldb.rest.store.CommandStore;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.streams.core.rstream.StreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 有个线程池，不断获取command，并执行
 */
@Service
public class Engin {
    private static final Logger logger = LoggerFactory.getLogger(Engin.class);

    private static final String PRODUCER_GROUP = "RSQL_PRODUCER_GROUP";

    private final RSQLConfig rsqlConfig;
    private final CommandStore commandStore;
    private final TaskFactory taskFactory;
    private final DefaultMQProducer producer;
    private final ThreadPoolExecutor executor;
    private final BlockingQueue<Runnable> cacheQueue = new LinkedBlockingQueue<Runnable>();


    public Engin(CommandStore commandStore, RSQLConfig rsqlConfig, TaskFactory taskFactory) {
        this.rsqlConfig = rsqlConfig;
        this.commandStore = commandStore;
        this.taskFactory = taskFactory;
        this.producer = this.producer();
        this.executor = new ThreadPoolExecutor(
                rsqlConfig.getThreadNum(),
                rsqlConfig.getThreadNum(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.cacheQueue,
                new ThreadFactoryImpl("RSQL_EnginThread_"));
    }


    public void start() {
        this.taskFactory.init(commandStore::findTable);

        try {
            producer.start();
        } catch (MQClientException e) {
            throw new RSQLServerException(e);
        }

        while (true) {
            try {
                Node nextCommand = this.commandStore.getNextCommand();
                //任务开始后才能提交消费位点；

                this.executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            //
                            StreamBuilder builder = new StreamBuilder("");
                            if (nextCommand instanceof Statement) {
                                taskFactory.dispatch((Statement) nextCommand, new BuildContext(producer, builder));
                            }

                        } catch (Throwable t) {
                            //不能忽略
                        }
                    }
                });

            } catch (Throwable t) {
                logger.error("", t);
            }

        }
    }

    private DefaultMQProducer producer() {
        String nameSrvAddr = this.rsqlConfig.namesrvAddr;
        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP);
        producer.setNamesrvAddr(nameSrvAddr);
        try {
            producer.start();
        } catch (MQClientException e) {
            throw new RSQLServerException("producer start error, namesrv: " + nameSrvAddr, e);
        }
        return producer;
    }


}
