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

import com.alibaba.rsqldb.parser.model.Calculator;
import com.alibaba.rsqldb.parser.model.Field;
import com.alibaba.rsqldb.parser.model.Operator;
import com.alibaba.rsqldb.parser.model.baseType.Literal;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

//一定会和groupby一起使用
// HAVING aggregate_function(column_name) operator value;
@JsonIgnoreProperties(ignoreUnknown = true)
public class SingleValueCalcuExpression extends SingleValueExpression {
    private static final Logger logger = LoggerFactory.getLogger(SingleValueCalcuExpression.class);

    private Calculator calculator;
    private Map<String/*tableName@fieldName@asFieldName*/, String/*asName*/> field2AsName = new HashMap<>();

    @JsonCreator
    public SingleValueCalcuExpression(@JsonProperty("content") String content, @JsonProperty("field") Field field,
                                      @JsonProperty("operator") Operator operator, @JsonProperty("value") Literal<?> value,
                                      @JsonProperty("calculator") Calculator calculator) {
        super(content, field, operator, value);
        this.calculator = calculator;
    }

    public Calculator getCalculator() {
        return calculator;
    }

    public void setCalculator(Calculator calculator) {
        this.calculator = calculator;
    }

    @Override
    public boolean isTrue(JsonNode jsonNode) {
        if (jsonNode == null) {
            return false;
        }

        Field field = super.getField();
        String asFieldName = field.getAsFieldName();
        if (StringUtils.isBlank(asFieldName)) {
            logger.error("the asName can not be empty, it can not judge the having sentence.");
            return false;
        }

        return isTrue(jsonNode, asFieldName);
    }
}
