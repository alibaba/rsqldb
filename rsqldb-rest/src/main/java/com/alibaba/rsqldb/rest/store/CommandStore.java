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
import org.apache.rocketmq.streams.core.running.RocketMQClient;
import org.apache.rocketmq.streams.core.util.RocketMQUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;


@Service
public class CommandStore implements CommandQueue {
    private static final Logger logger = LoggerFactory.getLogger(CommandStore.class);

    private final RocketMQClient rocketMQClient;

    private final DefaultLitePullConsumer pullConsumer;
    private final DefaultMQProducer producer;
    private final DefaultMQAdminExt mqAdmin;

    private Collection<MessageQueue> mqSet;
    private final ConcurrentHashMap<String, CreateTableStatement> cache = new ConcurrentHashMap<>();


    public CommandStore(RSQLConfig rsqlConfig) {
        rocketMQClient = new RocketMQClient(rsqlConfig.getNamesrvAddr());

        pullConsumer = new DefaultLitePullConsumer(RSQLConfig.SQL_GROUP_NAME);
        pullConsumer.setPullBatchSize(1);
        pullConsumer.setNamesrvAddr(rsqlConfig.getNamesrvAddr());
        pullConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
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
            mqSet = pullConsumer.fetchMessageQueues(RSQLConfig.SQL_GROUP_NAME);


        } catch (Exception e) {
            throw new RSQLServerException("start localStore error.", e);
        }
    }

//    /**
//     * 相同的key取queueOffset最大的一条
//     * kev-value对需要按照写入顺序排列好
//     */
//    @Override
//    public void restore() {
//        List<MessageExt> holder = new ArrayList<>();
//        List<MessageExt> result = pullConsumer.poll(50);
//        while (result != null && result.size() != 0) {
//            holder.addAll(result);
//            if (holder.size() <= 1000) {
//                result = pullConsumer.poll(50);
//                continue;
//            }
//
//            replayState(holder);
//            holder.clear();
//
//            result = pullConsumer.poll(50);
//        }
//        if (holder.size() != 0) {
//            replayState(holder);
//        }
//    }


//    @Override
//    public void put(Node node) {
//        if (node == null) {
//            return;
//        }
//
//        Serializer serializer = SerializeTypeContainer.getSerializer(SerializeType.JSON);
//        byte[] bytes = serializer.serialize(node);
//
////        Deserializer deserializer = SerializeTypeContainer.getDeserializer(SerializeType.JSON);
////        Node deserialize = deserializer.deserialize(bytes, CreateTableStatement.class);
////        System.out.println(deserialize);
//
//        try {
//            Message message = new Message(RsqlConfig.SQL_TOPIC_NAME, bytes);
//            producer.send(message);
//        } catch (Throwable e) {
//            throw new RSQLServerException("put sql to sql topic error.", e);
//        }
//    }

    //todo compact topic 在static topic下行为表现
    @Override
    public void putCommand(Statement node) {
        if (node == null) {
            throw new IllegalArgumentException("table name or statement is null.");
        }
        Serializer serializer = SerializeTypeContainer.getSerializer(SerializeType.JSON);
        byte[] bytes = serializer.serialize(node);

        try {
            Message message = new Message(RSQLConfig.SQL_TOPIC_NAME, bytes);
            /**
             * 一个tableName只能对应一个statement，使用compact topic压缩，相同的key在一个queue里面
             */
            message.setKeys(node.getTableName());
            message.putUserProperty(RSQLConstant.BODY_TYPE, node.getClass().getName());
            producer.send(message, new SelectMessageQueueByHash(), node.getTableName());
        } catch (Throwable e) {
            throw new RSQLServerException("put sql to sql topic error.", e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Node getNextCommand() {
        List<MessageExt> messageExts = pullConsumer.poll();

        if (messageExts.size() > 1) {
            throw new RSQLServerException("unexpected error, command num more than 1.");
        }

        MessageExt command = messageExts.get(0);

        if (command == null) {
            return null;
        }

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

        return node;
    }

    @Override
    public CreateTableStatement findTable(String tableName) {
        if (cache.containsKey(tableName)) {
            return cache.get(tableName);
        }

        return null;
    }


//    private void replayState(List<MessageExt> msgs) {
//        if (msgs == null || msgs.size() == 0) {
//            return;
//        }
//
//        Map<String/*key*/, List<MessageExt>> groupByKey = msgs.stream().parallel().collect(Collectors.groupingBy(MessageExt::getKeys));
//        for (String key : groupByKey.keySet()) {
//            List<MessageExt> messageExts = groupByKey.get(key);
//            List<MessageExt> sortedMessages = sortByQueueOffset(messageExts);
//
//            //最后的消息
//            MessageExt result = sortedMessages.get(sortedMessages.size() - 1);
//
//            String emptyBody = result.getUserProperty(Constant.EMPTY_BODY);
//            if (Constant.TRUE.equals(emptyBody)) {
//                continue;
//            }
//
//            long bornTimestamp = result.getBornTimestamp();
//            byte[] body = result.getBody();
//
//            CacheCommand command = new CacheCommand(bornTimestamp, body);
//            this.cache.add(command);
//        }
//
//
//    }


//    private List<MessageExt> sortByQueueOffset(List<MessageExt> target) {
//        if (target == null || target.size() == 0) {
//            return new ArrayList<>();
//        }
//
//        target.sort((o1, o2) -> {
//            long diff = o1.getQueueOffset() - o2.getQueueOffset();
//
//            if (diff > 0) {
//                return 1;
//            }
//
//            if (diff < 0) {
//                return -1;
//            }
//            return 0;
//        });
//
//        return target;
//    }
}
