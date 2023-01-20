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
package com.alibaba.rsqldb.storage.rocketmq;

import com.alibaba.rsqldb.common.RSQLConstant;
import com.alibaba.rsqldb.common.exception.DeserializeException;
import com.alibaba.rsqldb.common.exception.RSQLServerException;
import com.alibaba.rsqldb.parser.model.Node;
import com.alibaba.rsqldb.parser.model.statement.CreateTableStatement;
import com.alibaba.rsqldb.parser.model.statement.CreateViewStatement;
import com.alibaba.rsqldb.parser.model.statement.Statement;
import com.alibaba.rsqldb.storage.api.Command;
import com.alibaba.rsqldb.storage.api.CommandQueue;
import com.alibaba.rsqldb.storage.api.CommandSerDe;
import com.alibaba.rsqldb.storage.api.CommandStatus;
import com.alibaba.rsqldb.storage.api.CommandWrapper;
import com.alibaba.rsqldb.storage.api.serialize.DefaultCommandSerDe;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


public class RocketMQStorage implements CommandQueue {
    private static final Logger logger = LoggerFactory.getLogger(RocketMQStorage.class);

    private RocketMQClient rocketMQClient;
    private String topicName;

    private DefaultLitePullConsumer pullConsumer;
    private DefaultMQProducer producer;
    private DefaultMQAdminExt mqAdmin;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final CommandSerDe commandSerDe = new DefaultCommandSerDe();

    private Collection<MessageQueue> commandMessageQueue;
    private final ConcurrentHashMap<String/*tableName*/, Statement> tableCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String/*jobId*/, CompletableFuture<Throwable>> preCommandMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String/*jobId*/, Command> commandMap = new ConcurrentHashMap<>();
    private final LinkedList<Command> restoreCommand = new LinkedList<>();

    @Override
    public void start(Properties properties) {
        String namesrv = properties.getProperty(RSQLConstant.RocketMQ.NAMESRV_ADDR);
        if (StringUtils.isBlank(namesrv)) {
            throw new RSQLServerException("namesrv can not be blank.");
        }

        String groupName = RSQLConstant.RocketMQ.SQL_GROUP_NAME;
        if (StringUtils.isBlank(groupName)) {
            throw new RSQLServerException("groupName can not be blank.");
        }

        topicName = RSQLConstant.RocketMQ.SQL_TOPIC_NAME;
        if (StringUtils.isBlank(topicName)) {
            throw new RSQLServerException("topicName can not be blank.");
        }

        build(namesrv, groupName);

        try {
            //创建逻辑分区数为1的topic，确保命令先进先出地执行。
            RocketMQUtil.createStaticCompactTopic(mqAdmin, topicName, 1, null);

            pullConsumer.start();
            producer.start();

            Collection<MessageQueue> messageQueues = pullConsumer.fetchMessageQueues(topicName);
            if (messageQueues == null || messageQueues.size() != 1) {
                throw new RSQLServerException("command topic queue not equals 1. messageQueue=" + messageQueues);
            }
            commandMessageQueue = messageQueues;
        } catch (Exception e) {
            throw new RSQLServerException("start localStore error.", e);
        }
    }

    public void build(String namesrv, String groupName) {
        rocketMQClient = new RocketMQClient(namesrv);

        pullConsumer = new DefaultLitePullConsumer(groupName);
        pullConsumer.setNamesrvAddr(namesrv);
        pullConsumer.setMessageModel(MessageModel.BROADCASTING);
        pullConsumer.setAutoCommit(false);

        producer = rocketMQClient.producer(groupName);

        try {
            mqAdmin = rocketMQClient.getMQAdmin();
        } catch (MQClientException e) {
            throw new RSQLServerException(e);
        }
    }

    @Override
    public CompletableFuture<Boolean> restore() throws Throwable {
        //恢复command topic中所有的命令到本地，存储建表语句
        pullConsumer.setPullBatchSize(1000);
        pullConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        pullConsumer.assign(commandMessageQueue);
        pullConsumer.seekToBegin((MessageQueue) commandMessageQueue.toArray()[0]);

        CompletableFuture<Boolean> completableFuture = new CompletableFuture<>();

        this.executor.submit(() -> {
            try {
                pullToLast();
                //pull into cache, but not execute the job，It not sure if that job will be executed success.
                //if not, it will be skipped.
                //if the ser-de changed, the pullToLast will raise exception, and not commit the offset.
                commit();
                pullConsumer.setPullBatchSize(1);

                completableFuture.complete(true);
            } catch (Throwable t) {
                completableFuture.complete(false);
                logger.error("pull to last error.", t);
                throw new RSQLServerException(t);
            }
        });

        return completableFuture;
    }

    @Override
    public CompletableFuture<Throwable> putCommand(Command command) throws Throwable {
        if (checkExist(command)) {
            CompletableFuture<Throwable> result = new CompletableFuture<>();
            result.complete(null);
            return result;
        }

        byte[] bytes = commandSerDe.serialize(command);

        String jobId = command.getJobId();

        try {
            Message message = new Message(topicName, bytes);
            message.setKeys(jobId);
            message.putUserProperty(RSQLConstant.BODY_TYPE, command.getClass().getName());

            producer.send(message);

            logger.info("put statement into rocketmq command topic:{} with jobId:[{}], command:[{}]",
                    topicName, jobId, command.getNode() == null ? null : command.getNode().getContent());
        } catch (Throwable e) {
            throw new RSQLServerException("put sql to command topic error.", e);
        }

        CompletableFuture<Throwable> completableFuture = new CompletableFuture<>();
        CompletableFuture<Throwable> oldFuture = this.preCommandMap.put(jobId, completableFuture);
        if (oldFuture != null) {
            logger.warn("find uncompleted completableFuture, completed it.");
            oldFuture.complete(null);
        }

        return completableFuture;
    }

    private boolean checkExist(Command command) throws Throwable {
        String jobId = command.getJobId();
        Command tempCommand = commandMap.get(jobId);

        if (tempCommand != null  && tempCommand.getStatus() == command.getStatus()) {
            String format = String.format("exist a command has same jobId and status in commandMap, not executed yet, exist command:[%s].", tempCommand);
            logger.error(format);
            throw new RSQLServerException(format);
        }

        CompletableFuture<Throwable> future = preCommandMap.get(jobId);
        if (future != null) {
            String format = String.format("exist a command with same jobId, not executed yet, exist command:[%s].", tempCommand);
            logger.error(format);
            throw new RSQLServerException(format);
        }
        return false;
    }


    @Override
    public CommandWrapper getNextCommand() throws Throwable {
        //先从restore恢复中拉去
        if (this.restoreCommand.size() != 0) {
            Command restored = this.restoreCommand.pop();

            CompletableFuture<Throwable> future = new CompletableFuture<>();
            future.complete(null);

            return new CommandWrapper(restored, new RocketMQCallBack(future, this::commitStatus));
        }

        //poll from rocketmq
        MessageExt messageExt = pollFromStore();
        if (messageExt == null) {
            return null;
        }

        Command command = deserializeAndSaveTable(messageExt);
        String jobId = command.getJobId();

        CompletableFuture<Throwable> remove = this.preCommandMap.remove(jobId);

        if (remove == null) {
            logger.info("CompletableFuture is empty in local, the command maybe submit in other RSQLDB instance, jobId:{}", jobId);
            for (String jobIdInPreCommandMap : preCommandMap.keySet()) {
                logger.info("jobId in preCommandMap: {}", jobIdInPreCommandMap);
            }
            remove = new CompletableFuture<>();
        }

        return new CommandWrapper(command, new RocketMQCallBack(remove, this::commitStatus, this::commit));
    }

    private final LinkedList<MessageExt> cache = new LinkedList<>();
    private MessageExt pollFromStore() {
        MessageExt messageExt;
        if (cache.size() != 0) {
            messageExt = cache.pop();
        } else {
            List<MessageExt> messageExts = pullConsumer.poll(10);
            if (messageExts == null || messageExts.size() == 0) {
                return null;
            }
            if (messageExts.size() > 1) {
                pullConsumer.setPullBatchSize(1);
                logger.info("batch message num greater than 1, cache it first.");
                cache.addAll(messageExts);
                messageExt = cache.pop();
            } else {
                messageExt = messageExts.get(0);
            }
        }

        return messageExt;
    }


    private void commitStatus(String jobId, Command command) {
        if (command.getStatus() == CommandStatus.REMOVED) {
            Command remove = this.commandMap.remove(jobId);
            if (remove != null) {
                logger.info("remove command from cache, command:[{}]", remove);
                Node node = remove.getNode();
                if (node instanceof CreateTableStatement || node instanceof CreateViewStatement) {
                    Statement statement = (Statement) node;
                    String tableName = statement.getTableName();
                    Statement removedStatement = this.tableCache.remove(tableName);
                    if (removedStatement != null) {
                        logger.warn("remove table from cache. tableName:{}", tableName);
                    }
                }
            }
            return;
        }

        Command old = this.commandMap.put(jobId, command);
        if (old != null) {
            logger.info("change command, jobId:{}, status from:[{}] to:[{}]", jobId, old.getStatus(), command.getStatus());
        }
    }


    @Override
    public Statement findTable(String tableName) {
        if (tableCache.containsKey(tableName)) {
            return tableCache.get(tableName);
        }

        throw new RSQLServerException("Statement with tableName=" + tableName + " not exist.");
    }

    @Override
    public Command queryStatus(String jobId) {
        Command command = this.commandMap.get(jobId);
        if (command != null) {
            return command;
        }

        CompletableFuture<Throwable> future = this.preCommandMap.get(jobId);
        try {
            if (future != null) {
                future.get(10, TimeUnit.SECONDS);
            }
        } catch (Throwable ignored) {
        }

        return null;
    }

    @Override
    public List<Command> queryStatus() {
        List<Command> result = new ArrayList<>();

        for (String jobId : commandMap.keySet()) {
            Command temp = commandMap.get(jobId);
            result.add(temp);
        }

        return result;
    }

    @Override
    public CompletableFuture<Throwable> delete(String jobId) throws Throwable {
        Command command = new Command(jobId, null, CommandStatus.REMOVED);

        return this.putCommand(command);
    }

    @Override
    public void close() throws Exception {
        this.producer.shutdown();
        this.pullConsumer.shutdown();
        this.mqAdmin.shutdown();
        this.executor.shutdown();
    }

    private void commit() {
        HashSet<MessageQueue> set = new HashSet<>(commandMessageQueue);
        pullConsumer.commit(set, true);
    }


    private void pullToLast() throws DeserializeException {
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

    private void replayState(List<MessageExt> msgs) throws DeserializeException {
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

            Command command = deserializeAndSaveTable(result);

            this.restoreCommand.add(command);
        }
    }

    @SuppressWarnings("unchecked")
    private Command deserializeAndSaveTable(MessageExt msg) throws DeserializeException {
        String clazzName = msg.getUserProperty(RSQLConstant.BODY_TYPE);
        if (!Command.class.getName().equals(clazzName)) {
            throw new DeserializeException("unknown class name: " + clazzName);
        }

        Command command = commandSerDe.deserialize(msg.getBody());
        Node node = command.getNode();

        //保存到本地内存
        if (node instanceof CreateTableStatement || node instanceof CreateViewStatement) {
            Statement statement = (Statement) node;
            tableCache.put(statement.getTableName(), statement);
        }

        return command;
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
