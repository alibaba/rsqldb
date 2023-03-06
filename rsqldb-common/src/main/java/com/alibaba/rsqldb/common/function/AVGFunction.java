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
package com.alibaba.rsqldb.common.function;

import com.alibaba.rsqldb.common.RSQLConstant;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AVGFunction implements SQLFunction {
    private static final Logger logger = LoggerFactory.getLogger(AVGFunction.class);
    private String tableName;
    private String fieldName;
    private String asName;

    @JsonCreator
    public AVGFunction(@JsonProperty("tableName") String tableName,
                       @JsonProperty("fieldName") String fieldName,
                       @JsonProperty("asName") String asName) {
        this.tableName = tableName;
        this.fieldName = fieldName;
        this.asName = asName;
    }

    @Override
    public void apply(JsonNode jsonNode, ConcurrentHashMap<String, Object> container) {
        JsonNode valueNode = getValue(jsonNode, tableName, fieldName);

        Number sum = (Number) container.get(sumField());
        Number count = (Number) container.get(countField());
        if (count == null) {
            count = new BigDecimal(1);
        } else {
            BigDecimal bigDecimal = new BigDecimal(String.valueOf(count));
            count = bigDecimal.add(new BigDecimal(1));
        }
        container.put(countField(), count);

        if (!(valueNode instanceof NumericNode)) {
            return;
        }

        String newValue = valueNode.asText();
        BigDecimal value = new BigDecimal(newValue);

        if (sum == null) {
            sum = value;
        } else {
            BigDecimal bigDecimal = new BigDecimal(String.valueOf(sum));
            sum = bigDecimal.add(value);
        }
        container.put(sumField(), sum);
    }

    @Override
    public void secondCalcu(ConcurrentHashMap<String, Object> container, Properties context) {
        Number sum = (Number) container.get(sumField());
        Number count = (Number) container.get(countField());

        if (count == null || "0".equals(String.valueOf(count))) {
            logger.error("the divided is zero or empty.");
            return;
        }

        BigDecimal sumBigDecimal = new BigDecimal(String.valueOf(sum));
        BigDecimal countBigDecimal = new BigDecimal(String.valueOf(count));

        container.put(asName, sumBigDecimal.divide(countBigDecimal, 2, RoundingMode.HALF_UP));
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
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
