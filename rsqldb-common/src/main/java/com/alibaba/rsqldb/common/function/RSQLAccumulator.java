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
package com.alibaba.rsqldb.common.function;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.rocketmq.streams.core.function.accumulator.Accumulator;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RSQLAccumulator implements Accumulator<JsonNode, ObjectNode> {
    private List<SQLFunction> sqlFunctions;
    private ConcurrentHashMap<String, Object> container = new ConcurrentHashMap<>();

    public RSQLAccumulator(@JsonProperty("sqlFunctions") List<SQLFunction> sqlFunctions) {
        this.sqlFunctions = sqlFunctions;
    }

    @Override
    public void addValue(JsonNode value) {
        if (value == null) {
            return;
        }

        for (SQLFunction function : sqlFunctions) {
            function.apply(value, container);
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
            function.secondCalcu(container, context);
        }

        ObjectNode node = JsonNodeFactory.instance.objectNode();
        for (String key : container.keySet()) {
            Object temp = container.get(key);
            if (temp instanceof BigDecimal) {
                BigDecimal value = (BigDecimal) temp;
                node.put(key, value.toString());
            } else if (temp instanceof String) {
                node.put(key, (String) temp);
            } else if (temp instanceof JsonNode) {
                node.set(key, (JsonNode) temp);
            } else {
                throw new UnsupportedOperationException();
            }
        }

        return node;
    }

    public List<SQLFunction> getSqlFunctions() {
        return sqlFunctions;
    }

    public void setSqlFunctions(List<SQLFunction> sqlFunctions) {
        this.sqlFunctions = sqlFunctions;
    }

    public ConcurrentHashMap<String, Object> getContainer() {
        return container;
    }

    public void setContainer(ConcurrentHashMap<String, Object> container) {
        this.container = container;
    }

    @Override
    public Accumulator<JsonNode, ObjectNode> clone() {
        return new RSQLAccumulator(new ArrayList<>(this.sqlFunctions));
    }


}
