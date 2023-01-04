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
import com.alibaba.rsqldb.parser.model.Node;
import com.alibaba.rsqldb.parser.model.statement.CreateTableStatement;
import com.alibaba.rsqldb.parser.model.statement.CreateViewStatement;
import com.alibaba.rsqldb.parser.model.statement.Statement;
import com.alibaba.rsqldb.parser.serialization.Deserializer;
import com.alibaba.rsqldb.parser.serialization.SerializeTypeContainer;
import com.alibaba.rsqldb.parser.serialization.Serializer;
import com.alibaba.rsqldb.rest.response.QueryResult;
import com.alibaba.rsqldb.rest.service.RSQLConfig;
import com.alibaba.rsqldb.rest.service.iml.CommandOperator;
import com.alibaba.rsqldb.rest.service.iml.RemoveNode;
import com.alibaba.rsqldb.rest.service.iml.RestartNode;
import com.alibaba.rsqldb.rest.service.iml.TerminateNode;
import org.apache.commons.lang3.StringUtils;
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
import org.apache.rocketmq.streams.core.util.Pair;
import org.apache.rocketmq.streams.core.util.RocketMQUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static com.alibaba.rsqldb.common.RSQLConstant.COMMAND_OPERATOR;


@Service
public class CommandStore implements CommandQueue {
    private static final Logger logger = LoggerFactory.getLogger(CommandStore.class);

    private final RocketMQClient rocketMQClient;

    private final DefaultLitePullConsumer pullConsumer;
    private final DefaultMQProducer producer;
    private final DefaultMQAdminExt mqAdmin;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    private Collection<MessageQueue> commandMessageQueue;
    private final ConcurrentHashMap<String/*tableName*/, Statement> tableCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String/*jobId*/, CommandResult> commandMap = new ConcurrentHashMap<>();
    private final LinkedList<CommandResult> restoreCommand = new LinkedList<>();

    private final ConcurrentHashMap<String/*jobId*/, Object> waitPoints = new ConcurrentHashMap<>();

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

    @Override
    public CompletableFuture<Throwable> putStatement(String jobId, Statement statement) {
        if (statement == null || StringUtils.isEmpty(jobId)) {
            throw new IllegalArgumentException("jobId or statement is null.");
        }

        if (commandMap.containsKey(jobId)) {
            throw new IllegalArgumentException("jobId exist, can not replace.");
        }


        Serializer serializer = SerializeTypeContainer.getSerializer(SerializeType.JSON);
        byte[] bytes = serializer.serialize(statement);

        try {
            Message message = new Message(RSQLConfig.SQL_TOPIC_NAME, bytes);
            message.setKeys(jobId);
            message.putUserProperty(RSQLConstant.BODY_TYPE, statement.getClass().getName());

            producer.send(message, new SelectMessageQueueByHash(), jobId);

            logger.info("put statement into rocketmq command topic:{} with jobId:[{}], command:[{}]", RSQLConfig.SQL_TOPIC_NAME, jobId, statement.getContent());
        } catch (Throwable e) {
            throw new RSQLServerException("put sql to command topic error.", e);
        }

        CompletableFuture<Throwable> result = new CompletableFuture<>();
        CommandResult commandResult = new CommandResult(jobId, CommandStatus.STORE, statement, result);
        commandMap.put(jobId, commandResult);

        return result;
    }

    //todo compact topic 在static topic下行为表现
    @Override
    public CompletableFuture<Throwable> putCommand(String jobId, CommandOperator operator) {
        if (operator == null || StringUtils.isEmpty(jobId)) {
            throw new IllegalArgumentException("jobId or status is null.");
        }

        if (!commandMap.containsKey(jobId)) {
            Object waitPoint = this.waitPoints.computeIfAbsent(jobId, value -> new Object());
            synchronized (waitPoint) {
                try {
                    waitPoint.wait(5_1000, 0);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        Node node = commandMap.get(jobId).getNode();

        Serializer serializer = SerializeTypeContainer.getSerializer(SerializeType.JSON);
        byte[] bytes = serializer.serialize(node);

        try {
            Message message = new Message(RSQLConfig.SQL_TOPIC_NAME, bytes);
            message.setKeys(jobId);//compact topic 根据这个进行compact
            message.putUserProperty(RSQLConstant.BODY_TYPE, node.getClass().getName());
            message.putUserProperty(COMMAND_OPERATOR, operator.name());
            if (operator == CommandOperator.REMOVE) {
                message.putUserProperty(Constant.EMPTY_BODY, Constant.TRUE);
            }

            producer.send(message, new SelectMessageQueueByHash(), jobId);

            logger.info("put command into rocketmq command topic:{} with jobId:[{}],CommandOperator:[{}] command:[{}]",
                    RSQLConfig.SQL_TOPIC_NAME, jobId, operator.name(), node.getContent());
        } catch (Throwable e) {
            throw new RSQLServerException("put sql to command topic error.", e);
        }

        CompletableFuture<Throwable> result = new CompletableFuture<>();
        CommandResult commandResult = new CommandResult(jobId, CommandStatus.STORE, node, result);
        commandMap.put(jobId, commandResult);

        return result;
    }

    @Override
    public Pair<String/*jobId*/, Node> getNextCommand() throws Exception {
        //先从restore恢复中拉去
        if (this.restoreCommand.size() != 0) {
            CommandResult result = this.restoreCommand.pop();
            this.commandMap.put(result.getJobId(), result);
            if (result.getStatus() != CommandStatus.TERMINATED) {
                return new Pair<String, Node>(result.getJobId(), result.getNode());
            }
        }


        List<MessageExt> messageExts = pullConsumer.poll(10);

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

        String jobId = command.getKeys();
        String tempOperator = command.getUserProperty(COMMAND_OPERATOR);

        Node node = deserializeAndSaveTable(command);
        save2Local(jobId, node);
        //notify
        Object waitPoint = this.waitPoints.remove(jobId);
        if (waitPoint != null) {
            synchronized (waitPoint) {
                waitPoint.notifyAll();
            }
        }

        //专供engin使用不会保存
        node = convert(tempOperator, jobId, node);

        return new Pair<>(jobId, node);
    }

    private Node convert(String tempOperator, String jobId, Node node) {
        if (StringUtils.isEmpty(tempOperator)) {
            return node;
        }
        CommandOperator commandOperator = CommandOperator.valueOf(tempOperator);
        switch (commandOperator) {
            case STOP: {
                return new TerminateNode(jobId, node.getContent());
            }
            case REMOVE: {
                return new RemoveNode(jobId, node.getContent());
            }
            case RESTART: {
                return new RestartNode(jobId, node.getContent());
            }
        }
        throw new IllegalArgumentException("unknown commandOperator type=" + commandOperator);
    }

    private void save2Local(String jobId, Node node) {
        CommandResult putCommandResult = this.commandMap.remove(jobId);

        CommandResult pollCommandResult;

        if (putCommandResult != null) {
            putCommandResult.setStatus(CommandStatus.CONSUMED);
            pollCommandResult = new CommandResult(jobId, CommandStatus.CONSUMED, node, putCommandResult.getPutCommandFuture());
        } else {
            pollCommandResult = new CommandResult(jobId, CommandStatus.CONSUMED, node);
        }

        this.commandMap.put(jobId, pollCommandResult);
    }

    @Override
    public Statement findTable(String tableName) {
        if (tableCache.containsKey(tableName)) {
            return tableCache.get(tableName);
        }

        throw new RSQLServerException("Statement with tableName=" + tableName + " not exist.");
    }

    @Override
    public QueryResult queryStatus(String jobId) {
        CommandResult result = this.commandMap.get(jobId);
        if (result != null) {
            return new QueryResult(jobId, result.getNode().getContent(), result.getStatus());
        }

        return null;
    }

    @Override
    public List<QueryResult> queryStatus() {
        List<QueryResult> result = new ArrayList<>();

        for (String jobId : commandMap.keySet()) {
            CommandResult temp = commandMap.get(jobId);
            QueryResult queryResult = new QueryResult(jobId, temp.getNode().getContent(), temp.getStatus());

            result.add(queryResult);
        }

        return result;
    }

    @Override
    public void remove(String jobId) {
        CommandResult result = this.commandMap.get(jobId);

        if (result != null) {
            result.onCompleted();
            Node node = result.getNode();
            if (node instanceof CreateTableStatement || node instanceof CreateViewStatement) {
                Statement statement = (Statement) node;
                String tableName = statement.getTableName();
                this.tableCache.remove(tableName);
                logger.warn("remove table statement by jobId [{}], statement=[{}]", jobId, node.getContent());
                //todo 检查是否有依赖
            }
            this.commandMap.remove(jobId);
        }
    }

    private void commit() {
        HashSet<MessageQueue> set = new HashSet<>(commandMessageQueue);
        pullConsumer.commit(set, true);
    }

    @Override
    public void onCompleted(String jobId, CommandStatus status) {
        //提交消费位点
        commit();

        if (status == null) {
            return;
        }

        //改变状态
        CommandResult result = this.commandMap.get(jobId);
        if (result != null) {
            result.setStatus(status);
            result.onCompleted();
        }
    }

    @Override
    public void onError(String jobId, CommandStatus status, Throwable attachment) {
        commit();

        CommandResult result = this.commandMap.get(jobId);
        if (result != null) {
            result.setStatus(status);
            result.onError(attachment);
        }
    }

    private void pullToLast() {
        List<MessageExt> holder = new ArrayList<>();
        //recover
        List<MessageExt> result = pullConsumer.poll(10);
        while (result != null && result.size() != 0) {
            holder.addAll(result);
            if (holder.size() <= 1000) {
                result = pullConsumer.poll(10);
                continue;
            }

            replayState(holder);
            holder.clear();

            result = pullConsumer.poll(10);
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
            String userProperty = result.getUserProperty(COMMAND_OPERATOR);
            String jobId = result.getKeys();

            if (Constant.TRUE.equals(emptyBody)) {
                continue;
            }

            Node node = deserializeAndSaveTable(result);

            CommandResult commandResult;
            if (!StringUtils.isEmpty(userProperty)) {
                CommandOperator operator = CommandOperator.valueOf(userProperty);
                switch (operator) {
                    case STOP: {
                        commandResult = new CommandResult(jobId, CommandStatus.TERMINATED, node);
                        break;
                    }
                    default: {
                        commandResult = new CommandResult(jobId, CommandStatus.RESTORE, node);
                        break;
                    }
                }
            } else {
                commandResult = new CommandResult(jobId, CommandStatus.RESTORE, node);
            }

            this.restoreCommand.add(commandResult);
        }
    }

    @SuppressWarnings("unchecked")
    private Node deserializeAndSaveTable(MessageExt command) {
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
        if (node instanceof CreateTableStatement || node instanceof CreateViewStatement) {
            Statement statement = (Statement) node;
            tableCache.put(statement.getTableName(), statement);
        }

        return node;
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
