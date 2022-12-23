///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package com.alibaba.rsqldb.rest.util;
//
//import com.alibaba.rsqldb.rest.service.RsqlConfig;
//import org.apache.rocketmq.client.exception.MQClientException;
//import org.apache.rocketmq.common.TopicConfig;
//import org.apache.rocketmq.common.protocol.ResponseCode;
//import org.apache.rocketmq.common.protocol.body.ClusterInfo;
//import org.apache.rocketmq.common.protocol.route.BrokerData;
//import org.apache.rocketmq.remoting.exception.RemotingException;
//import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.ForkJoinPool;
//
//public class RocketMQUtil {
//    private final Logger logger = LoggerFactory.getLogger(RocketMQUtil.class);
//    private final String namesrvAddr;
//    private DefaultMQAdminExt mqAdmin;
//    private List<String> existTopic = new ArrayList<>();
//
//    public RocketMQUtil(String namesrvAddr) {
//        this.namesrvAddr = namesrvAddr;
//        mqAdmin = new DefaultMQAdminExt();
//        mqAdmin.setNamesrvAddr(namesrvAddr);
//        try {
//            mqAdmin.start();
//        } catch (MQClientException e) {
//            throw new RuntimeException("create DefaultMQAdminExt error.", e);
//        }
//    }
//
//
//    public void createTopicIfNotExist(String topicName) {
//        if (existTopic.contains(topicName)) {
//            return;
//        }
//
//        //检查是否存在
//        try {
//            mqAdmin.examineTopicRouteInfo(topicName);
//            existTopic.add(topicName);
//            return;
//        } catch (RemotingException | InterruptedException e) {
//            logger.error("examine state topic route info error.", e);
//            throw new RuntimeException("examine state topic route info error.", e);
//        } catch (MQClientException exception) {
//            if (exception.getResponseCode() == ResponseCode.TOPIC_NOT_EXIST) {
//                logger.info("state topic does not exist, create it.");
//            } else {
//                throw new RuntimeException(exception);
//            }
//        }
//
//        //创建
//        try {
//            HashMap<String, String> brokerName2MaterBrokerAddr = this.getCluster(this.namesrvAddr);
//
//            for (String brokerName : brokerName2MaterBrokerAddr.keySet()) {
//                TopicConfig topicConfig = new TopicConfig(topicName, RsqlConfig.SQL_QUEUE_NUM, RsqlConfig.SQL_QUEUE_NUM);
//                HashMap<String, String> temp = new HashMap<>();
//                //todo 暂时不能支持；
////                temp.put("+delete.policy", "COMPACTION");
//                topicConfig.setAttributes(temp);
//                mqAdmin.createAndUpdateTopicConfig(brokerName2MaterBrokerAddr.get(brokerName), topicConfig);
//            }
//
//            existTopic.add(topicName);
//        } catch (Throwable t) {
//            logger.error("create SQL topic error.");
//            throw new RuntimeException("create SQL topic error.", t);
//        }
//    }
//
//    public HashMap<String/* brokerName */, String/*masterAddr*/> getCluster(String nameSrvAddr) {
//        DefaultMQAdminExt mqAdminExt = new DefaultMQAdminExt();
//        mqAdminExt.setNamesrvAddr(nameSrvAddr);
//        ClusterInfo clusterInfo = null;
//        try {
//            mqAdminExt.start();
//            clusterInfo = mqAdminExt.examineBrokerClusterInfo();
//        } catch (Exception e) {
//            throw new RuntimeException("get clusterInfo error.", e);
//        }
//        ForkJoinPool.commonPool().execute(mqAdminExt::shutdown);
//
//        Map<String/* brokerName */, BrokerData> brokerAddrTable = clusterInfo.getBrokerAddrTable();
//
//        HashMap<String/* brokerName */, String/*masterAddr*/> brokerName2MaterBrokerAddr = new HashMap<>();
//        for (String key : brokerAddrTable.keySet()) {
//            BrokerData brokerData = brokerAddrTable.get(key);
//            HashMap<Long, String> brokerAddrs = brokerData.getBrokerAddrs();
//            brokerName2MaterBrokerAddr.put(key, brokerAddrs.get(0L));
//        }
//        return brokerName2MaterBrokerAddr;
//    }
//}
