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

import com.alibaba.rsqldb.common.exception.SyntaxErrorException;
import com.alibaba.rsqldb.parser.model.Field;
import com.alibaba.rsqldb.parser.model.Operator;
import com.alibaba.rsqldb.parser.model.baseType.BooleanType;
import com.alibaba.rsqldb.parser.model.baseType.Literal;
import com.alibaba.rsqldb.parser.model.baseType.NumberType;
import com.alibaba.rsqldb.parser.model.baseType.StringType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SingleValueExpression extends SingleExpression {
    private static final String stringNull = "null";
    private Literal<?> value;

    @JsonCreator
    public SingleValueExpression(@JsonProperty("content") String content, @JsonProperty("field") Field field,
                                 @JsonProperty("operator") Operator operator, @JsonProperty("value") Literal<?> value) {
        super(content, field, operator);
        this.value = value;
    }

    public Literal<?> getValue() {
        return value;
    }

    public void setValue(Literal<?> value) {
        this.value = value;
    }

    @Override
    public boolean isTrue(JsonNode jsonNode) {
        String fieldName = this.getField().getFieldName();
        JsonNode node = jsonNode.get(fieldName);
        if (node == null || StringUtils.isBlank(node.asText()) || stringNull.equalsIgnoreCase(node.asText())) {
            return this.value == null;
        }


        switch (this.getOperator()) {
            case EQUAL: {
                try {
                    return super.isEqual(node, this.value);
                } catch (Throwable t) {
                    return false;
                }
            }
            default: {
                if (!(this.getOperator() == Operator.GREATER) && !(this.getOperator() == Operator.LESS)
                        && !(this.getOperator() == Operator.NOT_EQUAL) && !(this.getOperator() == Operator.GREATER_EQUAL)
                        && !(this.getOperator() == Operator.LESS_EQUAL)) {
                    throw new SyntaxErrorException("unknown operator type: " + this.getOperator());
                }

                if (!(this.value instanceof NumberType) || !(node instanceof NumericNode)) {
                    return false;
                }

                Double value = Double.valueOf(node.asText());
                Double target = Double.valueOf(String.valueOf(this.value.result()));

                return compare(this.getOperator(), value, target);
            }
        }
    }

    private boolean compare(Operator operator, Double value, Double target) {
        switch (operator) {
            case GREATER: {
                return value > target;
            }
            case LESS: {
                return value < target;
            }
            case NOT_EQUAL: {
                return !Objects.equals(value, target);
            }
            case GREATER_EQUAL: {
                return value >= target;
            }
            case LESS_EQUAL: {
                return value <= target;
            }
            default: {
                throw new SyntaxErrorException("unknown operator=" + operator);
            }
        }
    }
}
