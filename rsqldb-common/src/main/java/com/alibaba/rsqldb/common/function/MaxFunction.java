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

import com.fasterxml.jackson.databind.JsonNode;

import java.math.BigDecimal;
import java.util.concurrent.ConcurrentHashMap;

public class MaxFunction implements SQLFunction {
    private String fieldName;
    private String asName;

    public MaxFunction(String fieldName, String asName) {
        this.fieldName = fieldName;
        this.asName = asName;
    }

    @Override
    public void apply(JsonNode jsonNode, ConcurrentHashMap<String, Object> container) {
        JsonNode valueNode = jsonNode.get(fieldName);
        if (valueNode != null) {
            String value = valueNode.asText();
            BigDecimal newValue = new BigDecimal(value);

            if (!container.containsKey(asName)) {
                container.put(asName, newValue);
            }else {
                BigDecimal old = (BigDecimal)container.get(asName);
                BigDecimal max = old.max(newValue);

                container.put(asName, max);
            }
        }
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getAsName() {
        return asName;
    }

    public void setAsName(String asName) {
        this.asName = asName;
    }
}