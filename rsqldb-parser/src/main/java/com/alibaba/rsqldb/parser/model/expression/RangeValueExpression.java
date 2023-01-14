 /*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
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
package com.alibaba.rsqldb.parser.model.expression;

import com.alibaba.rsqldb.parser.model.Field;
import com.alibaba.rsqldb.parser.model.Operator;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RangeValueExpression extends SingleExpression {
    private long low;
    private long high;

    @JsonCreator
    public RangeValueExpression(@JsonProperty("content") String content, @JsonProperty("fieldName") Field field,
                                @JsonProperty("low") long low, @JsonProperty("high") long high) {
        super(content, field, Operator.BETWEEN_AND);
        this.low = low;
        this.high = high;
    }

    public long getLow() {
        return low;
    }

    public void setLow(long low) {
        this.low = low;
    }

    public long getHigh() {
        return high;
    }

    public void setHigh(long high) {
        this.high = high;
    }

    @Override
    public Operator getOperator() {
        return Operator.BETWEEN_AND;
    }

    @Override
    public boolean isTrue(JsonNode jsonNode) {
        String fieldName = this.getField().getFieldName();
        JsonNode node = jsonNode.get(fieldName);
        if (node == null) {
            return false;
        }

        long value = node.asLong();

        return low <= value && value <= high;
    }
}
