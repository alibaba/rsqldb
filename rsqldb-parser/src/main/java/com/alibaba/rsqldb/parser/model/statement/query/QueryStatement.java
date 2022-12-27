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
import com.alibaba.rsqldb.common.function.AVGFunction;
import com.alibaba.rsqldb.common.function.CountFunction;
import com.alibaba.rsqldb.common.function.EmptyFunction;
import com.alibaba.rsqldb.common.function.MaxFunction;
import com.alibaba.rsqldb.common.function.MinFunction;
import com.alibaba.rsqldb.common.function.RSQLAccumulator;
import com.alibaba.rsqldb.common.function.SQLFunction;
import com.alibaba.rsqldb.common.function.SumFunction;
import com.alibaba.rsqldb.common.function.WindowBoundaryTimeFunction;
import com.alibaba.rsqldb.parser.impl.BuildContext;
import com.alibaba.rsqldb.parser.model.Calculator;
import com.alibaba.rsqldb.parser.model.Field;
import com.alibaba.rsqldb.parser.model.expression.AndExpression;
import com.alibaba.rsqldb.parser.model.expression.Expression;
import com.alibaba.rsqldb.parser.model.expression.OrExpression;
import com.alibaba.rsqldb.parser.model.expression.SingleExpression;
import com.alibaba.rsqldb.parser.model.expression.SingleValueCalcuExpression;
import com.alibaba.rsqldb.parser.model.statement.Statement;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.streams.core.common.Constant;
import org.apache.rocketmq.streams.core.function.accumulator.Accumulator;
import org.apache.rocketmq.streams.core.rstream.GroupedStream;
import org.apache.rocketmq.streams.core.rstream.RStream;
import org.apache.rocketmq.streams.core.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 单纯的select * from 语句map中的value都是null，不会存在计算。
 */
public class QueryStatement extends Statement {
    private static final Logger logger = LoggerFactory.getLogger(QueryStatement.class);

    private Map<Field/*输出的字段*/, Calculator/*针对字段的计算方式，可能为null*/> selectFieldAndCalculator;

    private List<SQLFunction> sqlFunctions;

    public QueryStatement(String content, String tableName, Map<Field, Calculator> selectFieldAndCalculator) {
        super(content, tableName);
        if (selectFieldAndCalculator == null || selectFieldAndCalculator.size() == 0) {
            throw new SyntaxErrorException("select field is null. sql=" + this.getContent());
        }
        this.selectFieldAndCalculator = selectFieldAndCalculator;

        sqlFunctions = buildFunction();
    }

    private List<SQLFunction> buildFunction() {
        ArrayList<SQLFunction> result = new ArrayList<>();
        if (isSelectAll()) {
            return result;
        }

        for (Field field : selectFieldAndCalculator.keySet()) {
            Calculator calculator = selectFieldAndCalculator.get(field);

            String fieldName = field.getFieldName();
            String asName = !StringUtils.isEmpty(field.getAsFieldName()) ? field.getAsFieldName() : field.getFieldName();

            SQLFunction function;
            if (calculator == null) {
                function = new EmptyFunction(fieldName, asName);
            } else {
                switch (calculator) {
                    case COUNT: {
                        function = new CountFunction(fieldName, asName);
                        break;
                    }
                    case MAX: {
                        function = new MaxFunction(fieldName, asName);
                        break;
                    }
                    case MIN: {
                        function = new MinFunction(fieldName, asName);
                        break;
                    }
                    case SUM: {
                        function = new SumFunction(fieldName, asName);
                        break;
                    }
                    case AVG: {
                        function = new AVGFunction(fieldName, asName);
                        break;
                    }
                    case WINDOW_START: {
                        function = new WindowBoundaryTimeFunction(Constant.WINDOW_START_TIME, asName);
                        break;
                    }
                    case WINDOW_END: {
                        function = new WindowBoundaryTimeFunction(Constant.WINDOW_END_TIME, asName);
                        break;
                    }
                    default: {
                        throw new RSQLServerException("unknown calculator type=" + calculator);
                    }
                }
            }

            result.add(function);
        }

        return result;
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

        if (isSelectField()) {
            rStream = rStream.map(value -> map(value, fieldName2AsName()));
            context.setrStream(rStream);
        } else {
            //select * from table就是所有值都只能在一个实例上计算，不然结果不准确
            GroupedStream<String, ObjectNode> groupedStream = rStream.keyBy(value -> QueryStatement.this.getContent()).aggregate(buildSelect());
            context.setGroupedStream(groupedStream);
        }

        return context;
    }

    protected Accumulator<JsonNode, ObjectNode> buildSelect() {
        return new RSQLAccumulator(sqlFunctions);
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

    protected boolean isSelectAll() {
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

    //只过滤字段，没有对字段进行计算
    protected boolean isSelectField() {
        for (Field field : selectFieldAndCalculator.keySet()) {
            Calculator calculator = selectFieldAndCalculator.get(field);
            if (calculator != null) {
                return false;
            }
        }
        return true;
    }

    protected HashMap<String, String> fieldName2AsName() {
        Set<Field> fields = selectFieldAndCalculator.keySet();

        HashMap<String, String> result = new HashMap<>();
        for (Field field : fields) {
            String asName = !StringUtils.isEmpty(field.getAsFieldName()) ?  field.getAsFieldName() : field.getFieldName();
            result.put(field.getFieldName(), asName);
        }

        return result;
    }
}
