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
package com.alibaba.rsqldb.parser.impl;

import com.alibaba.rsqldb.parser.model.baseType.Literal;
import com.alibaba.rsqldb.parser.model.statement.CreateTableStatement;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.streams.core.rstream.GroupedStream;
import org.apache.rocketmq.streams.core.rstream.JoinedStream;
import org.apache.rocketmq.streams.core.rstream.RStream;
import org.apache.rocketmq.streams.core.rstream.StreamBuilder;
import org.apache.rocketmq.streams.core.rstream.WindowStream;

import java.util.HashMap;
import java.util.Map;

public class BuildContext {
    private DefaultMQProducer producer;
    private StreamBuilder streamBuilder;
    private ObjectMapper objectMapper = new ObjectMapper();
    private Map<String, Object> header = new HashMap<>();

    //------------------------------streams------------------------------
    private RStream<JsonNode> rStream;

    private GroupedStream<?, ?> groupedStream;

    private WindowStream<?, ?> windowStream;

    private JoinedStream<?, ?> joinedStream;

    //在build过程中生成的
    //-------------------------------context-----------------------------
    private CreateTableStatement createTableStatement;
    private byte[] insertValueData;


    public BuildContext(DefaultMQProducer producer, StreamBuilder streamBuilder) {
        this.producer = producer;
        this.streamBuilder = streamBuilder;
    }

    public StreamBuilder getStreamBuilder() {
        return streamBuilder;
    }

    public void setStreamBuilder(StreamBuilder streamBuilder) {
        this.streamBuilder = streamBuilder;
    }

    public RStream<JsonNode> getrStream() {
        return rStream;
    }

    public void setrStream(RStream<JsonNode> rStream) {
        this.rStream = rStream;
    }

    public GroupedStream<?, ?> getGroupedStream() {
        return groupedStream;
    }

    public void setGroupedStream(GroupedStream<?, ?> groupedStream) {
        this.groupedStream = groupedStream;
    }

    public WindowStream<?, ?> getWindowStream() {
        return windowStream;
    }

    public void setWindowStream(WindowStream<?, ?> windowStream) {
        this.windowStream = windowStream;
    }

    public JoinedStream<?, ?> getJoinedStream() {
        return joinedStream;
    }

    public void setJoinedStream(JoinedStream<?, ?> joinedStream) {
        this.joinedStream = joinedStream;
    }

    public CreateTableStatement getCreateTableStatement() {
        return createTableStatement;
    }

    public void setCreateTableStatement(CreateTableStatement createTableStatement) {
        this.createTableStatement = createTableStatement;
    }

    public DefaultMQProducer getProducer() {
        return producer;
    }

    public void setProducer(DefaultMQProducer producer) {
        this.producer = producer;
    }

    public ObjectMapper getObjectMapper() {
        return objectMapper;
    }

    public void putHeader(String key, Object value) {
        if (StringUtils.isEmpty(key) || value == null) {
            return;
        }

        this.header.put(key, value);
    }

    public Object getHeader(String key) {
        return this.header.get(key);
    }

    public byte[] getInsertValueData() {
        return insertValueData;
    }

    public void setInsertValueData(byte[] insertValueData) {
        this.insertValueData = insertValueData;
    }
}
