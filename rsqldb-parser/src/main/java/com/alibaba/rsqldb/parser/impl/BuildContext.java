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
package com.alibaba.rsqldb.parser.impl;

import com.alibaba.rsqldb.common.RSQLConstant;
import com.alibaba.rsqldb.parser.model.statement.CreateTableStatement;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.streams.core.metadata.StreamConfig;
import org.apache.rocketmq.streams.core.rstream.GroupedStream;
import org.apache.rocketmq.streams.core.rstream.JoinedStream;
import org.apache.rocketmq.streams.core.rstream.RStream;
import org.apache.rocketmq.streams.core.rstream.StreamBuilder;
import org.apache.rocketmq.streams.core.rstream.WindowStream;

import java.util.HashMap;
import java.util.Map;

public class BuildContext {
    private final DefaultMQProducer producer;
    private final StreamBuilder streamBuilder;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Map<String, Object> header = new HashMap<>();

    //------------------------------source------------------------------
    private final Map<String/*tableName*/, RStream<JsonNode>> rStreamSource = new HashMap<>();

    //--------------------------------生成结果---------------------------------
    private RStream<? extends JsonNode> rStreamResult;

    private GroupedStream<String, ? extends JsonNode> groupedStreamResult;

    private WindowStream<String, ? extends JsonNode> windowStreamResult;

    private JoinedStream<?, ?> joinedStreamResult;

    //在build过程中生成的
    //-------------------------------context-----------------------------
    private CreateTableStatement createTableStatement;
    private byte[] insertValueData;


    public BuildContext(DefaultMQProducer producer, String jobId) {
        this.producer = producer;
        this.streamBuilder = new StreamBuilder(jobId);
        this.objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .disable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
                .enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT)
                .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                .enable(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN)
                .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
    }

    public StreamBuilder getStreamBuilder() {
        return streamBuilder;
    }

    public RStream<? extends JsonNode> getrStreamResult() {
        return rStreamResult;
    }

    public void setrStreamResult(RStream<? extends JsonNode> rStreamResult) {
        this.rStreamResult = rStreamResult;
    }

    public RStream<JsonNode> getRStreamSource(String tableName) {
        return this.rStreamSource.get(tableName);
    }

    public void addRStreamSource(String tableName, RStream<JsonNode> rStream) {
        this.rStreamSource.put(tableName, rStream);
    }

    public GroupedStream<String, ? extends JsonNode> getGroupedStreamResult() {
        return groupedStreamResult;
    }

    public void setGroupedStreamResult(GroupedStream<String, ? extends JsonNode> groupedStreamResult) {
        this.groupedStreamResult = groupedStreamResult;
    }

    public WindowStream<String, ? extends JsonNode> getWindowStreamResult() {
        return windowStreamResult;
    }

    public void setWindowStreamResult(WindowStream<String, ? extends JsonNode> windowStreamResult) {
        this.windowStreamResult = windowStreamResult;
    }

    public JoinedStream<?, ?> getJoinedStreamResult() {
        return joinedStreamResult;
    }

    public void setJoinedStreamResult(JoinedStream<?, ?> joinedStreamResult) {
        this.joinedStreamResult = joinedStreamResult;
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

    public Map<String, Object> getConfigSetAtBuild() {
        HashMap<String, Object> result = new HashMap<>();

        for (String key : header.keySet()) {
            if (key.startsWith(RSQLConstant.CONFIG_PREFIX)) {
                String configKey = key.substring(RSQLConstant.CONFIG_PREFIX.length());
                Object value = header.get(key);

                result.put(configKey, value);
            }
        }

        return result;
    }

    public byte[] getInsertValueData() {
        return insertValueData;
    }

    public void setInsertValueData(byte[] insertValueData) {
        this.insertValueData = insertValueData;
    }
}
