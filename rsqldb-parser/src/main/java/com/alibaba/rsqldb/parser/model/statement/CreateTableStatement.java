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
package com.alibaba.rsqldb.parser.model.statement;

import com.alibaba.rsqldb.common.RSQLConstant;
import com.alibaba.rsqldb.common.SerializeType;
import com.alibaba.rsqldb.common.exception.SyntaxErrorException;
import com.alibaba.rsqldb.common.serialization.Deserializer;
import com.alibaba.rsqldb.common.serialization.SerializeTypeContainer;
import com.alibaba.rsqldb.parser.impl.BuildContext;
import com.alibaba.rsqldb.parser.model.Columns;
import com.alibaba.rsqldb.parser.model.FieldType;
import com.alibaba.rsqldb.parser.model.baseType.Literal;
import com.alibaba.rsqldb.parser.model.baseType.StringType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.streams.core.rstream.RStream;
import org.apache.rocketmq.streams.core.rstream.StreamBuilder;
import org.apache.rocketmq.streams.core.serialization.KeyValueSerializer;
import org.apache.rocketmq.streams.core.util.Pair;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 主要确定 表与topic的关系
 * topic中数据如何解析成 表中的字段
 */
public class CreateTableStatement extends Statement {
    private Columns columns;
    private List<Pair<String, Literal<?>>> properties;

    private String topicName;
    private SerializeType serializeType;


    @JsonCreator
    public CreateTableStatement(@JsonProperty("content") String content, @JsonProperty("tableName") String tableName,
                                @JsonProperty("columns") Columns columns,
                                @JsonProperty("properties") List<Pair<String, Literal<?>>> properties) {
        super(content, tableName);
        if (properties == null) {
            throw new SyntaxErrorException("properties is null in create table.");
        }

        this.columns = columns;
        this.properties = properties;

        //保证在实例化时就确保必要参数是ok的
        this.topicName = this.getTopicNameFromProperties();
        this.serializeType = this.getSerializeTypeFromProperties();
    }

    public Columns getColumns() {
        return columns;
    }

    public void setColumns(Columns columns) {
        this.columns = columns;
    }

    public List<Pair<String, Literal<?>>> getProperties() {
        return properties;
    }

    public void setProperties(List<Pair<String, Literal<?>>> properties) {
        this.properties = properties;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public SerializeType getSerializeType() {
        return serializeType;
    }

    public void setSerializeType(SerializeType serializeType) {
        this.serializeType = serializeType;
    }

    private String getTopicNameFromProperties() {
        if (topicName != null) {
            return topicName;
        }

        String topicName = null;
        for (Pair<String, Literal<?>> property : properties) {
            String key = property.getKey();
            Literal<?> value = property.getValue();
            if (RSQLConstant.Properties.TOPIC.equalsIgnoreCase(key)) {
                if (!(value instanceof StringType)) {
                    throw new SyntaxErrorException("topicName is not string.");
                }
                StringType temp = (StringType) value;
                topicName = temp.getResult();
            }
        }

        if (StringUtils.isEmpty(topicName)) {
            throw new SyntaxErrorException("topicName is null in create table.");
        }

        return topicName;
    }

    private SerializeType getSerializeTypeFromProperties() {
        if (this.serializeType != null) {
            return serializeType;
        }

        String dataFormat = null;
        SerializeType type = null;
        for (Pair<String, Literal<?>> property : properties) {
            String key = property.getKey();
            Literal<?> value = property.getValue();

            if (RSQLConstant.Properties.DATA_FORMAT.equalsIgnoreCase(key)) {
                if (!(value instanceof StringType)) {
                    throw new SyntaxErrorException("data_format is not string.");
                }
                StringType temp = (StringType) value;
                dataFormat = temp.getResult();
            }
        }

        if (StringUtils.isEmpty(dataFormat)) {
            throw new SyntaxErrorException("data_format is null in create table.");
        }

        try {
            type = SerializeType.valueOf(dataFormat.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new SyntaxErrorException("unsupported deserialize type: " + dataFormat, e);
        }


        return type;
    }

    @Override
    public BuildContext build(BuildContext context) throws Throwable {
        Set<String> fieldNames =  this.columns.getFields();

        if (context.getHeader(RSQLConstant.TABLE_TYPE) == RSQLConstant.TableType.SOURCE) {
            StreamBuilder builder = context.getStreamBuilder();
            RStream<JsonNode> rStream = builder.source(topicName, source -> {
                Deserializer deserializer = SerializeTypeContainer.getDeserializer(serializeType);
                JsonNode result = deserializer.deserialize(source);
                //过滤
                Iterator<Map.Entry<String, JsonNode>> entryIterator = result.fields();
                while (entryIterator.hasNext()) {
                    Map.Entry<String, JsonNode> next = entryIterator.next();
                    if (!fieldNames.contains(next.getKey())) {
                        entryIterator.remove();
                    }
                }

                return new Pair<>(null, result);
            });

            context.setrStream(rStream);
            context.setCreateTableStatement(this);
        } else if (context.getHeader(RSQLConstant.TABLE_TYPE) == RSQLConstant.TableType.SINK) {
            RStream<JsonNode> stream = context.getrStream();
            stream.sink(topicName, new KeyValueSerializer<Object, JsonNode>() {
                @Override
                public byte[] serialize(Object key, JsonNode data) throws Throwable {

                    //todo
                    return new byte[0];
                }
            });

        }


        return context;
    }

    @Override
    public String toString() {
        return "Table{" +
                "tableName='" + this.getTableName() + '\'' +
                ", columns=" + columns +
                ", properties=" + properties +
                '}';
    }
}
