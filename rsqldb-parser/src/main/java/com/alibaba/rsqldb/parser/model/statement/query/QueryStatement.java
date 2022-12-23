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
package com.alibaba.rsqldb.parser.model.statement.query;

import com.alibaba.rsqldb.common.RSQLConstant;
import com.alibaba.rsqldb.common.exception.RSQLServerException;
import com.alibaba.rsqldb.common.exception.SyntaxErrorException;
import com.alibaba.rsqldb.parser.impl.BuildContext;
import com.alibaba.rsqldb.parser.impl.ParserConstant;
import com.alibaba.rsqldb.parser.model.Calculator;
import com.alibaba.rsqldb.parser.model.Field;
import com.alibaba.rsqldb.parser.model.Operator;
import com.alibaba.rsqldb.parser.model.baseType.Literal;
import com.alibaba.rsqldb.parser.model.expression.AndExpression;
import com.alibaba.rsqldb.parser.model.expression.Expression;
import com.alibaba.rsqldb.parser.model.expression.MultiValueExpression;
import com.alibaba.rsqldb.parser.model.expression.OrExpression;
import com.alibaba.rsqldb.parser.model.expression.RangeValueExpression;
import com.alibaba.rsqldb.parser.model.expression.SingleExpression;
import com.alibaba.rsqldb.parser.model.expression.SingleValueCalcuExpression;
import com.alibaba.rsqldb.parser.model.expression.SingleValueExpression;
import com.alibaba.rsqldb.parser.model.statement.Statement;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.streams.core.function.AggregateAction;
import org.apache.rocketmq.streams.core.rstream.RStream;
import org.apache.rocketmq.streams.core.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 单纯的select * from 语句map中的value都是null，不会存在计算。
 */
public class QueryStatement extends Statement {
    private static final Logger logger = LoggerFactory.getLogger(QueryStatement.class);

    private Map<Field/*输出的字段*/, Calculator/*针对字段的计算方式，可能为null*/> selectFieldAndCalculator;

    public QueryStatement(String content, String tableName, Map<Field, Calculator> selectFieldAndCalculator) {
        super(content, tableName);
        if (selectFieldAndCalculator == null || selectFieldAndCalculator.size() == 0) {
            throw new SyntaxErrorException("select field is null. sql=" + this.getContent());
        }
        this.selectFieldAndCalculator = selectFieldAndCalculator;
    }

    public Map<Field, Calculator> getSelectFieldAndCalculator() {
        return selectFieldAndCalculator;
    }

    public void setSelectFieldAndCalculator(Map<Field, Calculator> selectFieldAndCalculator) {
        this.selectFieldAndCalculator = selectFieldAndCalculator;
    }

    //having 子句中的每一个元素也必须出现在select列表中
    protected List<Pair<Field, Calculator>> validate(Expression havingExpression) {
        List<Pair<Field, Calculator>> havingFields = new ArrayList<>();
        collect(havingExpression, havingFields);

        if (isSelectAll()) {
            //select * from...
            return havingFields;
        }

        for (Pair<Field, Calculator> pair : havingFields) {
            String name = pair.getKey().getFieldName();
            Calculator calculator = pair.getValue();

            Calculator calculatorInSelect = this.getCalculator(name);

            if (!inSelectField(name) || calculatorInSelect != calculator) {
                throw new SyntaxErrorException("field in having but not in select. sql=" + this.getContent());
            }
        }

        return havingFields;
    }

    private void collect(Expression havingExpression, List<Pair<Field, Calculator>> fields) {
        if (havingExpression instanceof AndExpression) {
            AndExpression andExpression = (AndExpression) havingExpression;
            collect(andExpression.getLeftExpression(), fields);
            collect(andExpression.getRightExpression(), fields);
        } else if (havingExpression instanceof OrExpression) {
            OrExpression orExpression = (OrExpression) havingExpression;
            collect(orExpression.getLeftExpression(), fields);
            collect(orExpression.getRightExpression(), fields);
        } else if (havingExpression instanceof SingleExpression) {
            SingleExpression expression = (SingleExpression) havingExpression;
            Field fieldName = expression.getFieldName();
            Calculator calculator = null;
            if (havingExpression instanceof SingleValueCalcuExpression) {
                SingleValueCalcuExpression valueCalcuExpression = (SingleValueCalcuExpression) havingExpression;
                calculator = valueCalcuExpression.getCalculator();
            }

            Pair<Field, Calculator> pair = new Pair<>(fieldName, calculator);
            fields.add(pair);
        }
    }

    @Override
    public BuildContext build(BuildContext context) throws Throwable {
        if (isSelectAll()) {
            return context;
        }

        RStream<JsonNode> rStream = context.getrStream();
        buildSelect(rStream);

        context.setrStream(rStream);
        return context;
    }


    private void buildSelect(RStream<JsonNode> stream) {
        stream.aggregate(new AggregateAction<String, JsonNode, ObjectNode>() {
            @Override
            public ObjectNode calculate(String key, JsonNode value, ObjectNode accumulator) {
                for (Field field : selectFieldAndCalculator.keySet()) {
                    Calculator calculator = selectFieldAndCalculator.get(field);

                    String fieldName = field.getFieldName();
                    String asName = !StringUtils.isEmpty(field.getAsFieldName()) ? field.getAsFieldName() : field.getFieldName();

                    JsonNode valueNode = value.get(fieldName);

                    JsonNode storeNode = accumulator.get(asName);

                    if (calculator == null) {
                        accumulator.set(asName, valueNode);
                        continue;
                    }

                    switch (calculator) {
                        case COUNT: {
                            if (valueNode != null || RSQLConstant.STAR.equals(fieldName)) {
                                if (storeNode == null) {
                                    accumulator.put(asName, 1);
                                } else {
                                    int count = storeNode.asInt();
                                    accumulator.put(asName, ++count);
                                }
                            }
                            break;
                        }

                        case MIN: {
                            if (valueNode != null && valueNode.isNumber()) {
                                double newValue = valueNode.doubleValue();
                                if (storeNode == null) {
                                    accumulator.put(asName, newValue);
                                } else {
                                    double oldValue = storeNode.doubleValue();
                                    accumulator.put(asName, Math.min(newValue, oldValue));
                                }
                            }

                            if (valueNode != null && !valueNode.isNumber()) {
                                logger.error("calculator min by field:[{}], but the value is not a number:[{}].", fieldName, valueNode);
                            }
                            break;
                        }

                        case MAX: {
                            if (valueNode != null && valueNode.isNumber()) {
                                double newValue = valueNode.doubleValue();
                                if (storeNode == null) {
                                    accumulator.put(asName, newValue);
                                } else {
                                    double oldValue = storeNode.doubleValue();
                                    accumulator.put(asName, Math.max(newValue, oldValue));
                                }
                            }

                            if (valueNode != null && !valueNode.isNumber()) {
                                logger.error("calculator min by field:[{}], but the value is not a number:[{}].", fieldName, valueNode);
                            }
                            break;
                        }

                        case SUM: {
                            if (valueNode != null && valueNode.isNumber()) {
                                double newValue = valueNode.doubleValue();
                                if (storeNode == null) {
                                    accumulator.put(asName, newValue);
                                } else {
                                    double oldValue = storeNode.doubleValue();
                                    accumulator.put(asName, newValue + oldValue);
                                }
                            }
                        }

                        //todo
                        case AVG: {
                            if (valueNode != null || RSQLConstant.STAR.equals(fieldName)) {

                                JsonNode countNode = accumulator.get(String.join("@", ParserConstant.COUNT, asName));
                                JsonNode sumNode = accumulator.get(String.join("@", ParserConstant.SUM, asName));

                                if (countNode == null || sumNode == null) {
                                    accumulator.put(String.join("@", ParserConstant.COUNT, asName), 1);
                                    if (valueNode != null) {
                                        double newValue = valueNode.doubleValue();
                                        accumulator.put(String.join("@", ParserConstant.SUM, asName), newValue);
                                    } else {
                                        accumulator.put(String.join("@", ParserConstant.SUM, asName), 0);
                                    }

                                } else {
                                    double count = countNode.doubleValue();
                                    double sum = sumNode.doubleValue();
                                    accumulator.put(String.join("@", ParserConstant.COUNT, asName), ++count);
                                    if (valueNode != null) {
                                        double newValue = valueNode.doubleValue();
                                        accumulator.put(String.join("@", ParserConstant.SUM, asName), sum + newValue);
                                    }
                                }
                            }
                        }
                    }

                }

                return null;
            }
        });
    }

    protected boolean filter(JsonNode jsonNode, Expression filter) {
        Operator operator = filter.getOperator();

        switch (operator) {
            case OR: {
                OrExpression orExpression = (OrExpression) filter;
                Expression leftExpression = orExpression.getLeftExpression();
                Expression rightExpression = orExpression.getRightExpression();
                return filter(jsonNode, leftExpression) || filter(jsonNode, rightExpression);
            }
            case AND: {
                AndExpression andExpression = (AndExpression) filter;
                Expression leftExpression = andExpression.getLeftExpression();
                Expression rightExpression = andExpression.getRightExpression();

                return filter(jsonNode, leftExpression) && filter(jsonNode, rightExpression);
            }
            case IN: {
                MultiValueExpression multiValueExpression = (MultiValueExpression) filter;
                String fieldName = multiValueExpression.getFieldName().getFieldName();

                List<Literal<?>> literals = multiValueExpression.getValues().getLiterals();
                JsonNode node = jsonNode.get(fieldName);

                String value = node.asText();
                for (Literal<?> literal : literals) {
                    String target = String.valueOf(literal.getResult());
                    if (StringUtils.equalsIgnoreCase(value, target)) {
                        return true;
                    }
                }
                return false;
            }
            case BETWEEN_AND: {
                RangeValueExpression rangeValueExpression = (RangeValueExpression) filter;
                long high = rangeValueExpression.getHigh();
                long low = rangeValueExpression.getLow();

                String fieldName = rangeValueExpression.getFieldName().getFieldName();
                JsonNode node = jsonNode.get(fieldName);

                long value = node.asLong();

                return low <= value && value <= high;
            }
            case EQUAL: {
                if (filter instanceof SingleValueExpression && !(filter instanceof SingleValueCalcuExpression)) {
                    SingleValueExpression singleValueExpression = (SingleValueExpression) filter;
                    String fieldName = singleValueExpression.getFieldName().getFieldName();
                    Literal<?> literal = singleValueExpression.getValue();

                    JsonNode node = jsonNode.get(fieldName);
                    String value = node.asText();
                    String target = String.valueOf(literal.getResult());

                    return StringUtils.equalsIgnoreCase(value, target);

                } else {
                    throw new RSQLServerException("unknown filter type: " + filter.getClass().getName() + ",filter content=" + filter.getContent());
                }
            }
            default: {
                if (!(operator == Operator.GREATER) && !(operator == Operator.LESS)
                        && !(operator == Operator.NOT_EQUAL) && !(operator == Operator.GREATER_EQUAL)
                        && !(operator == Operator.LESS_EQUAL)) {
                    throw new SyntaxErrorException("unknown operator type: " + operator);
                }

                if (filter instanceof SingleValueExpression && !(filter instanceof SingleValueCalcuExpression)) {
                    SingleValueExpression singleValueExpression = (SingleValueExpression) filter;
                    String fieldName = singleValueExpression.getFieldName().getFieldName();
                    Literal<?> literal = singleValueExpression.getValue();

                    JsonNode node = jsonNode.get(fieldName);
                    Double value = Double.valueOf(node.asText());
                    Double target = Double.valueOf(String.valueOf(literal.getResult()));

                    return compare(operator, value, target);
                } else {
                    throw new RSQLServerException("unknown filter type: " + filter.getClass().getName() + ",filter content=" + filter.getContent());
                }
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

    private boolean inSelectField(String fieldName) {
        if (StringUtils.isEmpty(fieldName)) {
            return false;
        }

        for (Field field : selectFieldAndCalculator.keySet()) {
            if (fieldName.equals(field.getFieldName())) {
                return true;
            }
        }
        return false;
    }

    private Calculator getCalculator(String fieldName) {
        if (StringUtils.isEmpty(fieldName)) {
            return null;
        }

        for (Field field : selectFieldAndCalculator.keySet()) {
            if (fieldName.equals(field.getFieldName())) {
                return selectFieldAndCalculator.get(field);
            }
        }
        return null;
    }

    private boolean isSelectAll() {
        if (selectFieldAndCalculator.size() == 1) {
            Set<Field> fields = selectFieldAndCalculator.keySet();
            for (Field field : fields) {
                if (RSQLConstant.STAR.equals(field.getFieldName())) {
                    return true;
                }
            }
        }

        return false;
    }
}
