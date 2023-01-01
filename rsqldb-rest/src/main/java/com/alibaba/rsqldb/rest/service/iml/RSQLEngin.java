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
import com.alibaba.rsqldb.rest.service.Engin;
import com.alibaba.rsqldb.rest.service.RSQLConfig;
import com.alibaba.rsqldb.rest.store.CommandQueue;
import com.alibaba.rsqldb.rest.store.CommandStatus;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.streams.core.RocketMQStream;
import org.apache.rocketmq.streams.core.common.Constant;
import org.apache.rocketmq.streams.core.topology.TopologyBuilder;
import org.apache.rocketmq.streams.core.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 有个线程池，不断获取command，并执行
 */
@Service
public class RSQLEngin implements Engin {
    private static final Logger logger = LoggerFactory.getLogger(RSQLEngin.class);

    private static final String PRODUCER_GROUP = "RSQL_PRODUCER_GROUP";

    private final RSQLConfig rsqlConfig;
    private final CommandQueue commandQueue;
    private final TaskFactory taskFactory;
    private final DefaultMQProducer producer;
    private final ThreadPoolExecutor executor;
    private final BlockingQueue<Runnable> cacheQueue = new LinkedBlockingQueue<Runnable>();

    private AtomicReference<RSQLServerException> holder = new AtomicReference<>();

    private volatile boolean stop = false;
    private HashMap<String, RocketMQStream> rStreams = new HashMap<>();

    public RSQLEngin(CommandQueue commandQueue, RSQLConfig rsqlConfig, TaskFactory taskFactory) {
        this.rsqlConfig = rsqlConfig;
        this.commandQueue = commandQueue;
        this.taskFactory = taskFactory;
        this.producer = this.producer();
        this.executor = new ThreadPoolExecutor(
                1,
                1,
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.cacheQueue,
                new ThreadFactoryImpl("RSQL_EnginThread_"));

        this.commandQueue.start();
        try {
            CompletableFuture<Boolean> future = this.commandQueue.restore();

            future.thenAcceptAsync(value -> {
                if (value != null && value) {
                    this.start();
                } else {
                    RSQLServerException exception = new RSQLServerException("can not start engin, because restore failed.");
                    holder.set(exception);
                }
            });
        } catch (Exception e) {
            throw new RSQLServerException(e);
        }

    }


    public void start() {
        this.taskFactory.init(commandQueue::findTable);

        try {
            producer.start();
        } catch (MQClientException e) {
            throw new RSQLServerException(e);
        }

        this.executor.submit(this::runInLoop);

        logger.info("start engin success!");
    }

    private void validate() {
        if (holder.get() != null) {
            throw holder.get();
        }
    }

    private void runInLoop() {
        while (!stop) {
            Pair<String/*jobId*/, Node> commandResult = null;
            try {
                commandResult = this.commandQueue.getNextCommand();
                if (commandResult == null) {
                    continue;
                }
                //任务开始后才能提交消费位点；
                Node nextCommand = commandResult.getValue();

                if (nextCommand instanceof Statement) {
                    String jobId = commandResult.getKey();
                    BuildContext context = new BuildContext(producer, jobId);

                    logger.info("start construct stream task, with jobId={}, command={}", jobId, nextCommand.getContent());

                    BuildContext dispatch = taskFactory.dispatch((Statement) nextCommand, context);

                    if (dispatch != null) {
                        TopologyBuilder topologyBuilder = dispatch.getStreamBuilder().build();

                        Properties properties = new Properties();
                        properties.put(MixAll.NAMESRV_ADDR_PROPERTY, rsqlConfig.getNamesrvAddr());

                        RocketMQStream rocketMQStream = new RocketMQStream(topologyBuilder, properties);
                        RocketMQStream previous = rStreams.put(jobId, rocketMQStream);
                        if (previous != null) {
                            logger.warn("jobId replaced, jobId=[{}], new sql content=[{}]", jobId, nextCommand.getContent());
                            previous.stop();
                        }

                        rocketMQStream.start();

                        logger.info("start a stream task, with jobId:[{}] and sql content=[{}]", jobId, nextCommand.getContent());
                    }

                    this.commandQueue.onCompleted(commandResult.getKey(), CommandStatus.RUNNING);
                } else if (nextCommand instanceof TerminateNode) {
                    TerminateNode command = (TerminateNode) nextCommand;
                    String jobId = command.getJobId();

                    RocketMQStream stream = this.rStreams.get(jobId);
                    if (stream != null) {
                        stream.stop();
                    }

                    this.commandQueue.onCompleted(jobId, CommandStatus.TERMINATED);
                } else if (nextCommand instanceof RestartNode) {
                    RestartNode node = (RestartNode) nextCommand;
                    String jobId = node.getJobId();

                    RocketMQStream stream = this.rStreams.get(jobId);

                    CommandStatus status = this.commandQueue.queryStatus(jobId);
                    if (stream != null && status == CommandStatus.TERMINATED) {
                        stream.start();
                        this.commandQueue.onCompleted(jobId, CommandStatus.RUNNING);
                    }
                } else if (nextCommand instanceof RemoveNode) {
                    RemoveNode node = (RemoveNode) nextCommand;
                    String jobId = node.getJobId();

                    RocketMQStream stream = this.rStreams.remove(jobId);
                    if (stream != null) {
                        stream.stop();
                    }
                    this.commandQueue.remove(jobId);

                    this.commandQueue.onCompleted(jobId, null);
                }
            } catch (Throwable t) {
                logger.error("execute command failed, this command will be skipped. content in command: [{}]", commandResult, t);
                if (commandResult != null) {
                    this.commandQueue.onError(commandResult.getKey(), CommandStatus.SKIPPED, t);
                }
            }
        }
    }

    @Override
    public CompletableFuture<Throwable> putStatement(String jobId, Statement statement) {
        validate();
        return this.commandQueue.putStatement(jobId, statement);
    }

    @Override
    public Map<String, CommandStatus> queryAll() {
        validate();
        return this.commandQueue.queryStatus();
    }

    @Override
    public CommandStatus queryByJobId(String jobId) {
        validate();
        return this.commandQueue.queryStatus(jobId);
    }

    @Override
    public void terminate(String jobId) {
        //发送任务终止命令到rocketmq
        CompletableFuture<Throwable> future = this.commandQueue.putCommand(jobId,  CommandOperator.STOP);

        wait4Finish(future);
    }

    @Override
    public void restart(String jobId) {
        validate();
        CompletableFuture<Throwable> future = this.commandQueue.putCommand(jobId, CommandOperator.RESTART);

        wait4Finish(future);
    }

    @Override
    public void remove(String jobId) {
        validate();
        CompletableFuture<Throwable> future = this.commandQueue.putCommand(jobId, CommandOperator.REMOVE);
        wait4Finish(future);
    }


    @PreDestroy
    public void shutdown() {
        this.stop = true;
        for (RocketMQStream stream : rStreams.values()) {
            stream.stop();
        }
        this.producer.shutdown();
        this.executor.shutdownNow();
    }

    private DefaultMQProducer producer() {
        String nameSrvAddr = this.rsqlConfig.namesrvAddr;
        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP);
        producer.setNamesrvAddr(nameSrvAddr);

        return producer;
    }

    private void wait4Finish(CompletableFuture<Throwable> future) {
        try {
            Throwable error = future.get(10, TimeUnit.SECONDS);
            if (error != null) {
                throw error;
            }
        } catch (Throwable e) {
            throw new RSQLServerException(e);
        }
    }
}
