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

import com.alibaba.rsqldb.parser.model.Field;
import com.alibaba.rsqldb.parser.model.Operator;
import com.alibaba.rsqldb.parser.model.WildcardType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.commons.lang3.StringUtils;

import java.util.Locale;

@JsonIgnoreProperties(ignoreUnknown = true)
public class WildcardExpression extends SingleExpression {
    private WildcardType type;
    private String target;
    private boolean caseSensitive = false;

    @JsonCreator
    public WildcardExpression(@JsonProperty("content") String content,
                              @JsonProperty("field") Field field,
                              @JsonProperty("operator") Operator operator,
                              @JsonProperty("type") WildcardType type,
                              @JsonProperty("target") String target,
                              @JsonProperty("caseSensitive") boolean caseSensitive) {
        super(content, field, operator);
        this.type = type;
        this.target = target;
        this.caseSensitive = caseSensitive;
    }

    public boolean isCaseSensitive() {
        return caseSensitive;
    }

    public void setCaseSensitive(boolean caseSensitive) {
        this.caseSensitive = caseSensitive;
    }

    public WildcardType getType() {
        return type;
    }

    public void setType(WildcardType type) {
        this.type = type;
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    @Override
    public boolean isTrue(JsonNode jsonNode) {
        if (jsonNode == null) {
            return false;
        }

        String fieldName = this.getField().getFieldName();
        JsonNode node = jsonNode.get(fieldName);

        if (node == null || StringUtils.isBlank(node.asText()) || "null".equalsIgnoreCase(node.asText())) {
            return false;
        }

        if (!(node instanceof TextNode)) {
            return false;
        }

        if (Operator.LIKE.equals(this.getOperator())) {
            return likeWildcard(node.asText());
        }

        return false;
    }

    private boolean likeWildcard(String value) {
        String tempTarget = target;
        String tempValue = value;

        if (!caseSensitive) {
            tempTarget = target.toLowerCase(Locale.ROOT);
            tempValue = value.toLowerCase(Locale.ROOT);
        }

        switch (type) {
            case PREFIX_LIKE: {
                if (tempValue.startsWith(tempTarget)) {
                    return true;
                }
                break;
            }
            case SUFFIX_LIKE: {
                if (tempValue.endsWith(tempTarget)) {
                    return true;
                }
                break;
            }
            case DOUBLE_LIKE: {
                if (tempValue.contains(tempTarget)) {
                    return true;
                }
                break;
            }
            default: {
                throw new UnsupportedOperationException();
            }

        }
        return false;
    }
}
