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

import com.alibaba.rsqldb.common.RSQLConstant;
import com.alibaba.rsqldb.common.exception.RSQLServerException;
import com.fasterxml.jackson.databind.JsonNode;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class AVGFunction implements SQLFunction {
    private String fieldName;
    private String asName;

    public AVGFunction(String fieldName, String asName) {
        this.fieldName = fieldName;
        this.asName = asName;
    }

    @Override
    public void apply(JsonNode jsonNode, ConcurrentHashMap<String, Object> container) {
        JsonNode valueNode = jsonNode.get(fieldName);

        if (valueNode == null) {
            return;
        }

        BigDecimal sum = (BigDecimal) container.get(sumField());
        BigDecimal count = (BigDecimal) container.get(countField());


        if (sum == null && count == null) {
            container.put(countField(), new BigDecimal(1));
            String newValue = valueNode.asText();
            BigDecimal value = new BigDecimal(newValue);

            container.put(sumField(), value);
        } else if (sum != null && count != null) {
            BigDecimal add = count.add(new BigDecimal(1));
            container.put(countField(), add);

            String node = valueNode.asText();
            BigDecimal value = new BigDecimal(node);

            BigDecimal newValue = sum.add(value);
            container.put(sumField(), newValue);
        } else {
            throw new RSQLServerException();
        }
    }

    @Override
    public void secondCalcu(ConcurrentHashMap<String, Object> container, Properties context) {
        BigDecimal sum = (BigDecimal) container.get(sumField());
        BigDecimal count = (BigDecimal) container.get(countField());

        container.put(asName, sum.divide(count, 2, RoundingMode.HALF_UP));
        container.remove(sumField());
        container.remove(countField());
    }

    private String sumField() {
        return String.join("@", RSQLConstant.SUM, asName);
    }

    private String countField() {
        return String.join("@", RSQLConstant.COUNT, asName);
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
