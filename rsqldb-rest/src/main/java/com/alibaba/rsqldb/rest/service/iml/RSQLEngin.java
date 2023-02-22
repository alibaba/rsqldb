/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rsqldb.rest.service.iml;

import com.alibaba.rsqldb.common.exception.RSQLClientException;
import com.alibaba.rsqldb.common.exception.RSQLServerException;
import com.alibaba.rsqldb.parser.impl.BuildContext;
import com.alibaba.rsqldb.parser.model.Node;
import com.alibaba.rsqldb.parser.model.statement.Statement;
import com.alibaba.rsqldb.rest.service.Engin;
import com.alibaba.rsqldb.rest.service.RSQLConfig;
import com.alibaba.rsqldb.rest.service.RSQLConfigBuilder;
import com.alibaba.rsqldb.rest.spi.ServiceLoader;
import com.alibaba.rsqldb.storage.api.CallBack;
import com.alibaba.rsqldb.storage.api.Command;
import com.alibaba.rsqldb.storage.api.CommandQueue;
import com.alibaba.rsqldb.storage.api.CommandStatus;
import com.alibaba.rsqldb.storage.api.CommandWrapper;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.streams.core.RocketMQStream;
import org.apache.rocketmq.streams.core.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;


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

    public RSQLEngin(RSQLConfigBuilder builder, TaskFactory taskFactory, ServiceLoader serviceLoader) {
        this.rsqlConfig = builder.build();
        this.taskFactory = taskFactory;
        this.producer = this.producer();
        this.executor = new ThreadPoolExecutor(
                1,
                1,
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.cacheQueue,
                new ThreadFactoryImpl("RSQL_EnginThread_"));

        try {
            this.commandQueue = serviceLoader.load(CommandQueue.class, rsqlConfig.getStorage());
        } catch (Exception e) {
            logger.error("load storage module error.", e);
            throw new RSQLServerException(e);
        }

        this.commandQueue.start(builder.getProperties());
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
        } catch (Throwable e) {
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
            Command command = null;
            CallBack callBack = null;
            String jobId = null;
            Throwable error = null;

            try {
                CommandWrapper commandWrapper = this.commandQueue.getNextCommand();
                if (commandWrapper == null) {
                    continue;
                }
                //任务开始后才能提交消费位点；
                command = commandWrapper.getCommand();
                jobId = command.getJobId();
                CommandStatus status = command.getStatus();

                callBack = commandWrapper.getCallBack();

                RocketMQStream stream = this.rStreams.get(jobId);

                switch (status) {
                    case RUNNING: {
                        if (stream != null) {
                            stream.start();
                        } else {
                            startStream(command);
                        }
                        break;
                    }
                    case STOPPED: {
                        if (stream != null) {
                            logger.info("stop stream task, jobId:{}", jobId);
                            stream.stop();
                        } else {
                            logger.warn("the RocketMQStream is null, jobId:[{}], status:[{}]", jobId, CommandStatus.STOPPED);
                        }
                        break;
                    }
                    case REMOVED: {
                        logger.info("remove stream task, jobId:{}", jobId);
                        stream = this.rStreams.remove(jobId);
                        if (stream != null) {
                            stream.stop();
                        } else {
                            logger.warn("the RocketMQStream is null, jobId:[{}], status:[{}]", jobId, CommandStatus.REMOVED);
                        }
                        break;
                    }
                    default: {
                        throw new RSQLServerException("unknown command status: " + status);
                    }
                }
            } catch (Throwable t) {
                error = t;
                logger.error("execute command failed, this command will be skipped. content in command: [{}]", command, t);
            } finally {
                if (callBack != null) {
                    if (error == null) {
                        callBack.onCompleted(jobId, command);
                    } else {
                        command.setStatus(CommandStatus.STOPPED);
                        callBack.onError(jobId, command, error);
                    }
                }

            }
        }
    }

    private void startStream(Command command) throws Throwable {
        String jobId = command.getJobId();
        Node node = command.getNode();

        BuildContext context = new BuildContext(producer, jobId);

        logger.info("【prepare stream task】, with jobId={}, command={}", jobId, node.getContent());

        BuildContext dispatch = taskFactory.dispatch((Statement) node, context);

        if (dispatch != null) {
            TopologyBuilder topologyBuilder = dispatch.getStreamBuilder().build();

            Properties properties = new Properties();
            properties.put(MixAll.NAMESRV_ADDR_PROPERTY, rsqlConfig.getNamesrvAddr());
            properties.putAll(dispatch.getConfigSetAtBuild());

            RocketMQStream rocketMQStream = new RocketMQStream(topologyBuilder, properties);
            RocketMQStream previous = rStreams.put(jobId, rocketMQStream);
            if (previous != null) {
                logger.warn("jobId replaced, jobId=[{}], new sql content=[{}]", jobId, node.getContent());
                previous.stop();
            }

            rocketMQStream.start();

            logger.info("【start stream task】, with jobId:[{}] and sql content=[{}]", jobId, node.getContent());
        } else {
            logger.info("non-runnable task, command:[{}]", command);
        }
    }

    @Override
    public CompletableFuture<Throwable> putCommand(String jobId, Node node, boolean startJob) throws Throwable {
        validate();

        Command command;
        if (startJob) {
            command = new Command(jobId, node, CommandStatus.RUNNING);
        } else {
            command = new Command(jobId, node, CommandStatus.STOPPED);
        }

        return this.commandQueue.putCommand(command);
    }

    @Override
    public List<Command> queryAll() {
        validate();
        return this.commandQueue.queryStatus();
    }

    @Override
    public Command queryByJobId(String jobId) {
        validate();
        return this.commandQueue.queryStatus(jobId);
    }

    @Override
    public void terminate(String jobId) throws Throwable {
        validate();
        //发送任务终止命令到rocketmq
        Command result = this.queryByJobId(jobId);
        if (result == null) {
            String format = String.format("the command is empty corresponding to jobId: %s", jobId);
            logger.error(format);
            throw new RSQLClientException(format);
        }

        if (result.getStatus() == CommandStatus.STOPPED) {
            String format = String.format("jobId=[%s] is terminated, does not need terminated.", jobId);
            logger.error(format);
            throw new RSQLClientException(format);
        }

        Command command = new Command(jobId, result.getNode(), CommandStatus.STOPPED);
        CompletableFuture<Throwable> future = this.commandQueue.putCommand(command);

        wait4Finish(future);
    }

    @Override
    public void restart(String jobId) throws Throwable {
        validate();
        Command result = this.queryByJobId(jobId);
        if (result == null) {
            String format = String.format("the command is empty corresponding to jobId: %s", jobId);
            logger.error(format);
            throw new RSQLClientException(format);
        }

        if (result.getStatus() == CommandStatus.RUNNING) {
            String format = String.format("jobId=[%s] is running, does not need restart.", jobId);
            logger.error(format);
            throw new RSQLClientException(format);
        }

        Command command = new Command(jobId, result.getNode(), CommandStatus.RUNNING);
        CompletableFuture<Throwable> future = this.commandQueue.putCommand(command);

        wait4Finish(future);
    }

    //todo 移除create table和create view时候要非常小心,因为可能有针对这个表的insert等操作；
    @Override
    public void remove(String jobId) throws Throwable {
        validate();
        Command result = this.queryByJobId(jobId);
        if (result == null) {
            String format = String.format("the command is empty corresponding to jobId: %s", jobId);
            logger.error(format);
            throw new RSQLClientException(format);
        }

        if (result.getStatus() == CommandStatus.RUNNING) {
            String format = String.format("jobId=[%s] is running, terminate it first.", jobId);
            logger.error(format);
            throw new RSQLClientException(format);
        }


        CompletableFuture<Throwable> future = this.commandQueue.delete(jobId);
        wait4Finish(future);
    }


    @PreDestroy
    public void shutdown() {
        this.stop = true;
        try {
            this.commandQueue.close();
        } catch (Exception e) {
        }

        for (RocketMQStream stream : rStreams.values()) {
            stream.stop();
        }
        this.producer.shutdown();
        this.executor.shutdownNow();
    }

    private DefaultMQProducer producer() {
        String nameSrvAddr = this.rsqlConfig.getNamesrvAddr();
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
