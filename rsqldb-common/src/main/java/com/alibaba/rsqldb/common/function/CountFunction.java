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
import com.fasterxml.jackson.databind.JsonNode;

import java.math.BigDecimal;
import java.util.concurrent.ConcurrentHashMap;

public class CountFunction implements SQLFunction {
    private String fieldName;
    private String asName;

    public CountFunction(String fieldName, String asName) {
        this.fieldName = fieldName;
        this.asName = asName;
    }

    @Override
    public void apply(JsonNode jsonNode, final ConcurrentHashMap<String, Object> container) {
        JsonNode valueNode = jsonNode.get(fieldName);

        if (valueNode != null || RSQLConstant.STAR.equals(fieldName)) {

            BigDecimal old = (BigDecimal)container.get(asName);

            if (old == null) {
                container.put(asName, new BigDecimal(1));
            } else {
                BigDecimal count = old.add(new BigDecimal(1));

                container.put(asName, count);
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