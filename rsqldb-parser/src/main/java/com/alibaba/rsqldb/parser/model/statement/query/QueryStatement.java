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
package com.alibaba.rsqldb.parser.model.statement.query;

import com.alibaba.rsqldb.common.RSQLConstant;
import com.alibaba.rsqldb.common.exception.RSQLServerException;
import com.alibaba.rsqldb.common.exception.SyntaxErrorException;
import com.alibaba.rsqldb.common.function.AVGFunction;
import com.alibaba.rsqldb.common.function.CountFunction;
import com.alibaba.rsqldb.common.function.EmptyFunction;
import com.alibaba.rsqldb.common.function.MaxFunction;
import com.alibaba.rsqldb.common.function.MinFunction;
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
import com.alibaba.rsqldb.parser.model.statement.query.join.JointGroupByHavingStatement;
import com.alibaba.rsqldb.parser.model.statement.query.join.JointGroupByStatement;
import com.alibaba.rsqldb.parser.model.statement.query.join.JointStatement;
import com.alibaba.rsqldb.parser.model.statement.query.join.JointWhereGBHavingStatement;
import com.alibaba.rsqldb.parser.model.statement.query.join.JointWhereGroupByStatement;
import com.alibaba.rsqldb.parser.model.statement.query.join.JointWhereStatement;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
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
import java.util.Objects;
import java.util.Set;


@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY
)
@JsonSubTypes({
        @JsonSubTypes.Type(value = FilterQueryStatement.class, name = "filterQueryStatement"),
        @JsonSubTypes.Type(value = GroupByQueryStatement.class, name = "groupByQueryStatement"),
        @JsonSubTypes.Type(value = JointGroupByHavingStatement.class, name = "jointGroupByHavingStatement"),
        @JsonSubTypes.Type(value = JointGroupByStatement.class, name = "jointGroupByStatement"),
        @JsonSubTypes.Type(value = JointStatement.class, name = "jointStatement"),
        @JsonSubTypes.Type(value = JointWhereGBHavingStatement.class, name = "jointWhereGBHavingStatement"),
        @JsonSubTypes.Type(value = JointWhereGroupByStatement.class, name = "jointWhereGroupByStatement"),
        @JsonSubTypes.Type(value = JointWhereStatement.class, name = "jointWhereStatement"),
        @JsonSubTypes.Type(value = WindowQueryStatement.class, name = "windowQueryStatement")
})
public class QueryStatement extends Statement {
    private static final Logger logger = LoggerFactory.getLogger(QueryStatement.class);

    private Map<Field/*输出的字段*/, Calculator/*针对字段的计算方式，可能为null*/> selectFieldAndCalculator;

    private List<SQLFunction> sqlFunctions;

    @JsonCreator
    public QueryStatement(@JsonProperty("content") String content, @JsonProperty("tableName") String tableName,
                          @JsonProperty("selectFieldAndCalculator") Map<Field, Calculator> selectFieldAndCalculator) {
        super(content, tableName);
        if (selectFieldAndCalculator == null || selectFieldAndCalculator.size() == 0) {
            throw new SyntaxErrorException("select field is null. sql=" + this.getContent());
        }
        this.selectFieldAndCalculator = selectFieldAndCalculator;

        sqlFunctions = buildFunction();
    }

    private List<SQLFunction> buildFunction() {
        ArrayList<SQLFunction> result = new ArrayList<>();
        if (isSelectField()) {
            return result;
        }

        for (Field field : selectFieldAndCalculator.keySet()) {
            Calculator calculator = selectFieldAndCalculator.get(field);

            String fieldName = field.getFieldName();
            String asName = getAsName(field);
            String newName = !StringUtils.isEmpty(asName) ? asName : field.getFieldName();

            SQLFunction function;
            if (calculator == null) {
                function = new EmptyFunction(fieldName, newName);
            } else {
                switch (calculator) {
                    case COUNT: {
                        function = new CountFunction(fieldName, newName);
                        break;
                    }
                    case MAX: {
                        if (RSQLConstant.STAR.equals(fieldName)) {
                            throw new SyntaxErrorException("syntax error: MAX(*)");
                        }
                        function = new MaxFunction(fieldName, newName);
                        break;
                    }
                    case MIN: {
                        if (RSQLConstant.STAR.equals(fieldName)) {
                            throw new SyntaxErrorException("syntax error: MIN(*)");
                        }
                        function = new MinFunction(fieldName, newName);
                        break;
                    }
                    case SUM: {
                        if (RSQLConstant.STAR.equals(fieldName)) {
                            throw new SyntaxErrorException("syntax error: SUM(*)");
                        }
                        function = new SumFunction(fieldName, newName);
                        break;
                    }
                    case AVG: {
                        if (RSQLConstant.STAR.equals(fieldName)) {
                            throw new SyntaxErrorException("syntax error: AVG(*)");
                        }
                        function = new AVGFunction(fieldName, newName);
                        break;
                    }
                    case WINDOW_START: {
                        function = new WindowBoundaryTimeFunction(Constant.WINDOW_START_TIME, newName);
                        break;
                    }
                    case WINDOW_END: {
                        function = new WindowBoundaryTimeFunction(Constant.WINDOW_END_TIME, newName);
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
    protected void validAndPrePareHavingExpression(Expression havingExpression) {
        if (havingExpression == null) {
            return;
        }

        if (isSelectAll()) {
            //select * from...
            throw new SyntaxErrorException("select * can not occur in having sql, sql=" + this.getContent());
        }

        if (havingExpression instanceof AndExpression) {
            AndExpression andExpression = (AndExpression) havingExpression;
            validAndPrePareHavingExpression(andExpression.getLeftExpression());
            validAndPrePareHavingExpression(andExpression.getRightExpression());
        } else if (havingExpression instanceof OrExpression) {
            OrExpression orExpression = (OrExpression) havingExpression;
            validAndPrePareHavingExpression(orExpression.getLeftExpression());
            validAndPrePareHavingExpression(orExpression.getRightExpression());
        } else if (havingExpression instanceof SingleValueCalcuExpression) {
            SingleValueCalcuExpression calcuExpression = (SingleValueCalcuExpression) havingExpression;
            Field havingField = calcuExpression.getField();
            String fieldName = havingField.getFieldName();

            HashMap<String, String> fieldName2AsName = this.fieldName2AsName();

            String asName;
            if (!checkInSelect(fieldName, calcuExpression.getCalculator())) {
                throw new SyntaxErrorException("field in having but not in select. sql=" + this.getContent());
            } else {
                asName = fieldName2AsName.get(fieldName);
            }

            /**
             * use to judge the truth of having sentence {@link SingleValueCalcuExpression#isTrue(JsonNode)}
             */
            havingField.setAsFieldName(asName);
        }
    }


    @Override
    public BuildContext build(BuildContext context) throws Throwable {
        if (isSelectAll()) {
            return context;
        }

        RStream<JsonNode> rStream = context.getRStreamSource(this.getTableName());

        buildSelectItem(rStream, context);

        return context;
    }

    protected void buildSelectItem(RStream<JsonNode> rStream, BuildContext context) {
        if (isSelectField()) {
            rStream = rStream.map(value -> map(value, fieldName2AsName()));
            context.setrStreamResult(rStream);
        } else {
            //select class, avg(score) from table就是所有值都只能在一个实例上计算，不然结果不准确
            GroupedStream<String, ObjectNode> groupedStream = rStream.keyBy(value -> QueryStatement.this.getContent()).aggregate(buildAccumulator());
            context.setGroupedStreamResult(groupedStream);
        }
    }


    protected Accumulator<JsonNode, ObjectNode> buildAccumulator() {
        return new RSQLAccumulator(sqlFunctions);
    }

    private boolean checkInSelect(String fieldName, Calculator checkCalculator) {
        if (StringUtils.isEmpty(fieldName) || checkCalculator == null) {
            return false;
        }

        for (Field field : selectFieldAndCalculator.keySet()) {
            Calculator calculator = selectFieldAndCalculator.get(field);

            if (fieldName.equals(field.getFieldName()) && checkCalculator == calculator) {
                return true;
            }
        }
        return false;
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
            String asName = getAsName(field);
            String newName = !StringUtils.isEmpty(asName) ? asName : field.getFieldName();
            result.put(field.getFieldName(), newName);
        }

        return result;
    }

    private String getAsName(Field field) {
        String asFieldName = field.getAsFieldName();
        if (StringUtils.isEmpty(asFieldName) || "null".equalsIgnoreCase(asFieldName)) {
            return null;
        }

        return asFieldName;
    }


}
