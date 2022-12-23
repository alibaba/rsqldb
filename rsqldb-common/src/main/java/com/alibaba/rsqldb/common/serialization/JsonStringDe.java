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
//package com.alibaba.rsqldb.common.serialization;
//
//import com.alibaba.rsqldb.common.exception.DeserializeException;
//import com.fasterxml.jackson.databind.JsonNode;
//import com.fasterxml.jackson.databind.ObjectMapper;
//
//import java.io.IOException;
//import java.nio.charset.StandardCharsets;
//
//public class JsonStringDe implements Deserializer {
//    private static final ObjectMapper objectMapper = new ObjectMapper();
//
//    @Override
//    public JsonNode deserialize(byte[] source) {
//        if (source == null || source.length == 0) {
//            return null;
//        }
//
//        try {
//            String str = new String(source, StandardCharsets.UTF_8);
//            return objectMapper.readTree(str);
//        } catch (IOException e) {
//            throw new DeserializeException(e);
//        }
//
//    }
//}
