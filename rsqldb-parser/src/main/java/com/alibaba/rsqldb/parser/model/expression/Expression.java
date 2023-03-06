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
package com.alibaba.rsqldb.parser.model.expression;


import com.alibaba.rsqldb.parser.model.Node;
import com.alibaba.rsqldb.parser.model.Operator;
import com.alibaba.rsqldb.parser.model.baseType.BooleanType;
import com.alibaba.rsqldb.parser.model.baseType.Literal;
import com.alibaba.rsqldb.parser.model.baseType.NumberType;
import com.alibaba.rsqldb.parser.model.baseType.StringType;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.TextNode;

import java.util.Objects;

// fieldName > 10 and AVG(fieldName) < 12 ...
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY
)
@JsonSubTypes({
        @JsonSubTypes.Type(value = SingleValueExpression.class, name = "singleValueExpression"),
        @JsonSubTypes.Type(value = SingleValueCalcuExpression.class, name = "singleValueCalcuExpression"),
        @JsonSubTypes.Type(value = RangeValueExpression.class, name = "rangeValueExpression"),
        @JsonSubTypes.Type(value = MultiValueExpression.class, name = "multiValueExpression"),
        @JsonSubTypes.Type(value = OrExpression.class, name = "orExpression"),
        @JsonSubTypes.Type(value = AndExpression.class, name = "andExpression"),
        @JsonSubTypes.Type(value = WildcardExpression.class, name = "wildcardExpression")
})
public abstract class Expression extends Node {
    public Expression(String content) {
        super(content);
    }

    public abstract Operator getOperator();

    /**
     * true 表达式成立
     * @param jsonNode
     * @return
     */
    public abstract boolean isTrue(JsonNode jsonNode);


    protected boolean isEqual(JsonNode node, Literal<?> literal) {
        if (node == null && literal == null) {
            return true;
        }

        if (node == null || literal == null) {
            return false;
        }

        String value = node.asText();
        String target = String.valueOf(literal.result());

        if (literal instanceof StringType) {
            if (!(node instanceof TextNode)) {
                return false;
            }

            return Objects.equals(value, target);
        } else if (literal instanceof NumberType) {
            if (!(node instanceof NumericNode)) {
                return false;
            }

            return Objects.equals(value, target);
        } else if (literal instanceof BooleanType) {
            if (!(node instanceof BooleanNode)) {
                return false;
            }

            return Objects.equals(value, target);
        }

        return false;
    }
}
