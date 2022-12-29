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
package com.alibaba.rsqldb.rest.store;

import com.alibaba.rsqldb.common.RSQLConstant;
import com.alibaba.rsqldb.common.SerializeType;
import com.alibaba.rsqldb.common.exception.RSQLServerException;
import com.alibaba.rsqldb.common.serialization.Deserializer;
import com.alibaba.rsqldb.common.serialization.SerializeTypeContainer;
import com.alibaba.rsqldb.common.serialization.Serializer;
import com.alibaba.rsqldb.parser.model.Node;
import com.alibaba.rsqldb.parser.model.statement.CreateTableStatement;
import com.alibaba.rsqldb.parser.model.statement.CreateViewStatement;
import com.alibaba.rsqldb.parser.model.statement.Statement;
import com.alibaba.rsqldb.rest.service.RSQLConfig;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.selector.SelectMessageQueueByHash;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.streams.core.common.Constant;
import org.apache.rocketmq.streams.core.running.RocketMQClient;
import org.apache.rocketmq.streams.core.util.RocketMQUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;


@Service
public class CommandStore implements CommandQueue {
    private static final Logger logger = LoggerFactory.getLogger(CommandStore.class);

    private final RocketMQClient rocketMQClient;

    private final DefaultLitePullConsumer pullConsumer;
    private final DefaultMQProducer producer;
    private final DefaultMQAdminExt mqAdmin;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    private Collection<MessageQueue> commandMessageQueue;
    private final ConcurrentHashMap<String/*tableName*/, Statement> cache = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String/*jobId*/, CommandResult> putCommandMap = new ConcurrentHashMap<>();


    public CommandStore(RSQLConfig rsqlConfig) {
        rocketMQClient = new RocketMQClient(rsqlConfig.getNamesrvAddr());

        pullConsumer = new DefaultLitePullConsumer(RSQLConfig.SQL_GROUP_NAME);
        pullConsumer.setNamesrvAddr(rsqlConfig.getNamesrvAddr());
        pullConsumer.setMessageModel(MessageModel.BROADCASTING);
        pullConsumer.setAutoCommit(false);

        producer = rocketMQClient.producer(RSQLConfig.SQL_GROUP_NAME);

        try {
            mqAdmin = rocketMQClient.getMQAdmin();
        } catch (MQClientException e) {
            throw new RSQLServerException(e);
        }
    }

    @Override
    public void start() {
        try {
            //创建逻辑分区数为1的topic，确保命令先进先出地执行。
            RocketMQUtil.createStaticCompactTopic(mqAdmin, RSQLConfig.SQL_TOPIC_NAME, 1, null);

            pullConsumer.start();
            producer.start();

            Collection<MessageQueue> messageQueues = pullConsumer.fetchMessageQueues(RSQLConfig.SQL_TOPIC_NAME);
            if (messageQueues == null || messageQueues.size() != 1) {
                throw new RSQLServerException("command topic queue not equals 1. messageQueue=" + messageQueues);
            }
            commandMessageQueue = messageQueues;
        } catch (Exception e) {
            throw new RSQLServerException("start localStore error.", e);
        }
    }

    @Override
    public CompletableFuture<Boolean> restore() throws Exception {
        //恢复command topic中所有的命令到本地，存储建表语句
        pullConsumer.setPullBatchSize(1000);
        pullConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        pullConsumer.assign(commandMessageQueue);
        pullConsumer.seekToBegin((MessageQueue) commandMessageQueue.toArray()[0]);

        CompletableFuture<Boolean> completableFuture = new CompletableFuture<>();

        this.executor.submit(() -> {
            try {
                pullToLast();
                commit();
                pullConsumer.setPullBatchSize(1);

                completableFuture.complete(true);
            } catch (Throwable t) {
                completableFuture.complete(false);
                logger.error("pull to last error.", t);
                throw t;
            }
        });

        return completableFuture;
    }

    //todo compact topic 在static topic下行为表现
    @Override
    public CommandResult putCommand(String jobId, Node node) {
        if (node == null) {
            throw new IllegalArgumentException("table name or statement is null.");
        }

        if (putCommandMap.containsKey(jobId)) {
            throw new IllegalArgumentException("jobId exist.");
        }

        Serializer serializer = SerializeTypeContainer.getSerializer(SerializeType.JSON);
        byte[] bytes = serializer.serialize(node);

        try {
            Message message = new Message(RSQLConfig.SQL_TOPIC_NAME, bytes);

            message.setKeys(jobId);
            message.putUserProperty(RSQLConstant.BODY_TYPE, node.getClass().getName());
            producer.send(message, new SelectMessageQueueByHash(), jobId);

            logger.info("put command into rocketmq command topic:{} with jobId:[{}], command:[{}]", RSQLConfig.SQL_TOPIC_NAME, jobId, node.getContent());
        } catch (Throwable e) {
            throw new RSQLServerException("put sql to sql topic error.", e);
        }

        CompletableFuture<Boolean> result = new CompletableFuture<>();
        CommandResult commandResult = new CommandResult(jobId, CommandStatus.STORE, node, result);
        putCommandMap.put(jobId, commandResult);

        return commandResult;
    }

    @Override
    public CommandResult getNextCommand() throws Exception {
        List<MessageExt> messageExts = pullConsumer.poll();

        if (messageExts == null || messageExts.size() == 0) {
            return null;
        }

        if (messageExts.size() > 1) {
            throw new RSQLServerException("unexpected error, command num more than 1.");
        }

        MessageExt command = messageExts.get(0);

        if (command == null) {
            return null;
        }

        Node node = deserializeAndSave(command);

        String jobId = command.getKeys();

        CommandResult putCommandResult = this.putCommandMap.get(jobId);

        if (putCommandResult != null) {
            putCommandResult.setStatus(CommandStatus.CONSUMED);
            putCommandResult.complete();
        } else {
            putCommandResult = new CommandResult(jobId, CommandStatus.CONSUMED, node);
            this.putCommandMap.put(jobId, putCommandResult);
        }


        return putCommandResult;
    }

    @Override
    public Statement findTable(String tableName) {
        if (cache.containsKey(tableName)) {
            return cache.get(tableName);
        }

        throw new RSQLServerException("Statement with tableName=" + tableName + " not exist.");
    }

    @Override
    public Map<String, CommandResult> queryAll() {
        return Collections.unmodifiableMap(this.putCommandMap);
    }

    private void pullToLast() {
        List<MessageExt> holder = new ArrayList<>();
        //recover
        List<MessageExt> result = pullConsumer.poll(100);
        while (result != null && result.size() != 0) {
            holder.addAll(result);
            if (holder.size() <= 1000) {
                result = pullConsumer.poll(100);
                continue;
            }

            replayState(holder);
            holder.clear();

            result = pullConsumer.poll(100);
        }

        if (holder.size() != 0) {
            replayState(holder);
        }
    }

    private void replayState(List<MessageExt> msgs) {
        if (msgs == null || msgs.size() == 0) {
            return;
        }

        Map<String, List<MessageExt>> collect = msgs.stream().parallel().collect(Collectors.groupingBy(MessageExt::getKeys));
        for (String key : collect.keySet()) {
            List<MessageExt> messageExts = collect.get(key);

            List<MessageExt> sortedMessages = sortByQueueOffset(messageExts);

            //最后的消息
            MessageExt result = sortedMessages.get(sortedMessages.size() - 1);

            String emptyBody = result.getUserProperty(Constant.EMPTY_BODY);
            if (Constant.TRUE.equals(emptyBody)) {
                continue;
            }
            deserializeAndSave(result);
        }
    }

    @SuppressWarnings("unchecked")
    private Node deserializeAndSave(MessageExt command) {
        String clazzName = command.getUserProperty(RSQLConstant.BODY_TYPE);
        Class<Node> clazz = null;
        try {
            clazz = (Class<Node>) Class.forName(clazzName);
        } catch (ClassNotFoundException e) {
            throw new RSQLServerException(e);
        }

        Deserializer deserializer = SerializeTypeContainer.getDeserializer(SerializeType.JSON);

        Node node = deserializer.deserialize(command.getBody(), clazz);

        //保存到本地内存
        if (node instanceof CreateTableStatement) {
            CreateTableStatement statement = (CreateTableStatement) node;
            cache.put(statement.getTableName(), statement);
        }

        if (node instanceof CreateViewStatement) {
            CreateViewStatement statement = (CreateViewStatement) node;
            cache.put(statement.getTableName(), statement);
        }
        return node;
    }

    @Override
    public void changeCommandStatus(String jobId, CommandStatus status) {
        CommandResult result = this.putCommandMap.get(jobId);
        if (result != null) {
            result.setStatus(status);
        }
    }

    @Override
    public void changeCommandStatus(String jobId, CommandStatus status, Object attachment) {
        CommandResult result = this.putCommandMap.get(jobId);
        if (result != null) {
            result.setStatus(status);
            result.setAttachment(attachment);
        }
    }

    public void commit() {
        HashSet<MessageQueue> set = new HashSet<>(commandMessageQueue);
        pullConsumer.commit(set, true);
    }

    private List<MessageExt> sortByQueueOffset(List<MessageExt> target) {
        if (target == null || target.size() == 0) {
            return new ArrayList<>();
        }

        target.sort((o1, o2) -> {
            long diff = o1.getQueueOffset() - o2.getQueueOffset();

            if (diff > 0) {
                return 1;
            }

            if (diff < 0) {
                return -1;
            }
            return 0;
        });

        return target;
    }
}
