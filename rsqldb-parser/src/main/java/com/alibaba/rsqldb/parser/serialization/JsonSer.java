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
package com.alibaba.rsqldb.parser.serialization;

import com.alibaba.rsqldb.common.exception.SerializeException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JsonSer implements Serializer {
    private static final String KEY = "key";
    private static final String VALUE = "value";
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public byte[] serialize(Object obj) throws SerializeException {
        if (obj == null) {
            return new byte[0];
        }
        try {
            return objectMapper.writeValueAsBytes(obj);
        } catch (JsonProcessingException e) {
            throw new SerializeException(e);
        }
    }

    @Override
    public byte[] serialize(Object key, Object value) throws SerializeException {
        if (key == null) {
            return this.serialize(value);
        }

        try {
            ObjectNode objectNode = objectMapper.createObjectNode();

            String valueAsString = objectMapper.writeValueAsString(value);
            JsonNode valueJsonNode = objectMapper.readTree(valueAsString);

            if (key.getClass().isPrimitive()) {
                objectNode.set(String.valueOf(key), valueJsonNode);
            } else {
                throw new UnsupportedOperationException("key is not primitive.");
            }

            String result = objectNode.toPrettyString();

            return objectMapper.writeValueAsBytes(result);
        } catch (Throwable t) {
            throw new SerializeException(t);
        }
    }
}
