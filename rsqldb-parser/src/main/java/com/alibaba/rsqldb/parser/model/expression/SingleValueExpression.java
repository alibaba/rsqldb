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
package com.alibaba.rsqldb.parser.model.expression;

import com.alibaba.rsqldb.common.exception.SyntaxErrorException;
import com.alibaba.rsqldb.parser.model.Field;
import com.alibaba.rsqldb.parser.model.Operator;
import com.alibaba.rsqldb.parser.model.baseType.Literal;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.StringUtils;

public class SingleValueExpression extends SingleExpression {
    private Literal<?> value;

    public SingleValueExpression(String content, Field field, Operator operator, Literal<?> value) {
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
        String fieldName = this.getFieldName().getFieldName();

        switch (this.getOperator()) {
            case EQUAL: {
                JsonNode node = jsonNode.get(fieldName);

                String value = node.asText();
                String target = String.valueOf(this.value.getResult());

                return StringUtils.equalsIgnoreCase(value, target);
            }
            default: {
                if (!(this.getOperator() == Operator.GREATER) && !(this.getOperator() == Operator.LESS)
                        && !(this.getOperator() == Operator.NOT_EQUAL) && !(this.getOperator() == Operator.GREATER_EQUAL)
                        && !(this.getOperator() == Operator.LESS_EQUAL)) {
                    throw new SyntaxErrorException("unknown operator type: " + this.getOperator());
                }
                JsonNode node = jsonNode.get(fieldName);
                Double value = Double.valueOf(node.asText());
                Double target = Double.valueOf(String.valueOf(this.value.getResult()));

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
                return value != target;
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
