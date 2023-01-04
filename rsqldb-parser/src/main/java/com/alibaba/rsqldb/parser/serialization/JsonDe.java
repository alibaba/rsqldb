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

import com.alibaba.rsqldb.common.exception.DeserializeException;
import com.alibaba.rsqldb.parser.model.Field;
import com.alibaba.rsqldb.parser.model.expression.Expression;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.io.IOException;


public class JsonDe implements Deserializer {
    private final ObjectMapper objectMapper = new ObjectMapper();

    public JsonDe() {
        SimpleModule simpleModule = new SimpleModule();
        simpleModule.addKeyDeserializer(Field.class, new FieldKeyDeserializer());
        objectMapper.registerModule(simpleModule);
    }

    public JsonNode deserialize(byte[] source) throws DeserializeException {
        if (source == null || source.length == 0) {
            return null;
        }

        try {
            return objectMapper.readTree(source);
        } catch (IOException e) {
            throw new DeserializeException(e);
        }
    }

    @Override
    public <T> T deserialize(byte[] source, Class<T> clazz) {
        if (source == null || source.length == 0) {
            return null;
        }

        try {
            return objectMapper.readValue(source, clazz);
        } catch (IOException e) {
            throw new DeserializeException(e);
        }
    }
}
