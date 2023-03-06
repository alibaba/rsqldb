/*
 * Copyright 1999-2023 Alibaba Group Holding Ltd.
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
package com.alibaba.rsqldb.parser.model.statement.query;

import com.alibaba.rsqldb.common.function.SQLFunction;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.rocketmq.streams.core.function.accumulator.Accumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

 @JsonIgnoreProperties(ignoreUnknown = true)
 public class RSQLAccumulator implements Accumulator<JsonNode, ObjectNode> {
     private static final Logger logger = LoggerFactory.getLogger(RSQLAccumulator.class);

     private final List<SQLFunction> sqlFunctions;
     private final ConcurrentHashMap<String/*fieldName@asName@SQLFunction.getClass.getName*/, SQLFunction> sqlFunctionMap;
     private final ConcurrentHashMap<String, Object> tempHolder = new ConcurrentHashMap<>();

     @JsonCreator
     public RSQLAccumulator(@JsonProperty("sqlFunctions") List<SQLFunction> sqlFunctions) {
         this.sqlFunctions = sqlFunctions;
         this.sqlFunctionMap = buildSQLMap();
     }

     private ConcurrentHashMap<String, SQLFunction> buildSQLMap() {
         ConcurrentHashMap<String, SQLFunction> result = new ConcurrentHashMap<>();

         if (sqlFunctions == null || sqlFunctions.size() == 0) {
             return result;
         }

         for (SQLFunction sqlFunction : sqlFunctions) {
             String tableName = sqlFunction.getTableName();
             String fieldName = sqlFunction.getFieldName();
             String asName = sqlFunction.getAsName();
             String className = sqlFunction.getClass().getName();

             String key = String.join("@", tableName, fieldName, asName, className);

             result.putIfAbsent(key, sqlFunction);
         }

         return result;
     }

     @Override
     public void addValue(JsonNode value) {
         if (value == null) {
             return;
         }

         for (SQLFunction function : sqlFunctionMap.values()) {
             function.apply(value, tempHolder);
         }
     }

     @Override
     public void merge(Accumulator<JsonNode, ObjectNode> other) {

     }

     //触发窗口时调用
     @Override
     public ObjectNode result(Properties context) {
         //需要二次计算的，进行二次计算
         for (SQLFunction function : sqlFunctions) {
             function.secondCalcu(tempHolder, context);
         }

         ObjectNode node = JsonNodeFactory.instance.objectNode();


         for (SQLFunction sqlFunction : sqlFunctions) {
             String asName = sqlFunction.getAsName();

             Object temp = tempHolder.get(asName);

             if (temp instanceof Number) {
                 BigDecimal value = new BigDecimal(String.valueOf(temp));

                 String valueStr = value.toString();
                 if (valueStr.contains(".")) {
                     node.put(asName, value.doubleValue());
                 } else {
                     node.put(asName, value.longValue());
                 }

             } else if (temp instanceof String) {
                 node.put(asName, (String) temp);
             } else if (temp instanceof JsonNode) {
                 node.set(asName, (JsonNode) temp);
             } else if (temp == null) {
                 node.set(asName, null);
             } else {
                 logger.error("unsupported type: " + temp.getClass());
                 throw new UnsupportedOperationException();
             }
         }

         tempHolder.clear();

         return node;
     }

     public List<SQLFunction> getSqlFunctions() {
         return sqlFunctions;
     }

     public ConcurrentHashMap<String, Object> getTempHolder() {
         return tempHolder;
     }

     @Override
     public Accumulator<JsonNode, ObjectNode> clone() {
         return new RSQLAccumulator(new ArrayList<>(this.sqlFunctions));
     }


 }
