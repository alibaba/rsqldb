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

import com.alibaba.rsqldb.common.exception.SyntaxErrorException;
import com.alibaba.rsqldb.parser.impl.BuildContext;
import com.alibaba.rsqldb.parser.model.Calculator;
import com.alibaba.rsqldb.parser.model.Field;
import com.alibaba.rsqldb.parser.model.expression.Expression;
import com.alibaba.rsqldb.parser.model.statement.query.phrase.ExpressionType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.streams.core.common.Constant;
import org.apache.rocketmq.streams.core.function.accumulator.Accumulator;
import org.apache.rocketmq.streams.core.rstream.GroupedStream;
import org.apache.rocketmq.streams.core.rstream.RStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

//聚合查询
@JsonIgnoreProperties(ignoreUnknown = true)
public class GroupByQueryStatement extends QueryStatement {
    private static final Logger logger = LoggerFactory.getLogger(GroupByQueryStatement.class);

    private Expression whereExpression;
    private Expression havingExpression;
    //groupBy 后跟的字段
    private List<Field> groupByField;

    public GroupByQueryStatement(String content, String tableName,
                                 Map<Field, Calculator> selectFieldAndCalculator, List<Field> groupByField) {
        super(content, tableName, selectFieldAndCalculator);

        this.groupByField = groupByField;
        validate();
    }

    public GroupByQueryStatement(String content, String tableName,
                                 Map<Field, Calculator> selectFieldAndCalculator,
                                 List<Field> groupByField, Expression expression, ExpressionType expressionType) {
        super(content, tableName, selectFieldAndCalculator);

        if (expressionType == ExpressionType.HAVING) {
            this.havingExpression = expression;
        } else if (expressionType == ExpressionType.WHERE) {
            this.whereExpression = expression;
        } else {
            throw new IllegalArgumentException("unknown expressionType=" + expressionType);
        }
        this.groupByField = groupByField;
        validate();
        super.validate(havingExpression);
    }

    @JsonCreator
    public GroupByQueryStatement(@JsonProperty("content") String content, @JsonProperty("tableName") String tableName,
                                 @JsonProperty("selectFieldAndCalculator") Map<Field, Calculator> selectFieldAndCalculator,
                                 @JsonProperty("groupByField") List<Field> groupByField,
                                 @JsonProperty("whereExpression") Expression whereExpression,
                                 @JsonProperty("havingExpression") Expression havingExpression) {
        super(content, tableName, selectFieldAndCalculator);

        this.whereExpression = whereExpression;
        this.havingExpression = havingExpression;
        this.groupByField = groupByField;
        validate();
        super.validate(havingExpression);
    }

    /**
     * 【否】groupBy字段包含于select字段
     * groupBy字段的表名 = tableName
     * 【否】select上的字段，如果没有计算符，必须在groupBy上
     */
    private void validate() {
        if (groupByField == null || groupByField.size() == 0) {
            throw new SyntaxErrorException("groupBy field is null. sql=" + this.getContent());
        }

        for (Field field : groupByField) {
            if (!StringUtils.isEmpty(field.getTableName()) && !field.getTableName().equals(this.getTableName())) {
                throw new SyntaxErrorException("table name in groupBy are incorrect. sql=" + this.getContent());
            }
            if (StringUtils.isEmpty(field.getFieldName())) {
                throw new SyntaxErrorException("groupBy field name is null. sql=" + this.getContent());
            }
        }
    }

    public Expression getWhereExpression() {
        return whereExpression;
    }

    public void setWhereExpression(Expression whereExpression) {
        this.whereExpression = whereExpression;
    }

    public Expression getHavingExpression() {
        return havingExpression;
    }

    public void setHavingExpression(Expression havingExpression) {
        this.havingExpression = havingExpression;
    }

    public List<Field> getGroupByField() {
        return groupByField;
    }

    public void setGroupByField(List<Field> groupByField) {
        this.groupByField = groupByField;
    }


    @Override
    public BuildContext build(BuildContext context) throws Throwable {
        GroupedStream<String, JsonNode> groupedStream = buildGroupBy(context.getRStreamSource(this.getTableName()));

        //select
        GroupedStream<String, ? extends JsonNode> selectField = groupedStream;
        if (!isSelectAll()) {
            Accumulator<JsonNode, ObjectNode> select = buildAccumulator();
            selectField = groupedStream.aggregate(select);
        }

        //having
        selectField = buildHaving(selectField);

        context.setGroupedStreamResult(selectField);

        return context;
    }


    protected GroupedStream<String, JsonNode> buildGroupBy(RStream<JsonNode> stream) {

        //where 过滤
        if (whereExpression != null) {
            stream = stream.filter(value -> {
                try {
                    return whereExpression.isTrue(value);
                } catch (Throwable t) {
                    //使用错误，例如字段是string，使用>过滤；
                    logger.info("where filter error, sql:[{}], value=[{}]", GroupByQueryStatement.this.getContent(), value, t);
                    return false;
                }
            });
        }

        // groupBy
        return stream.keyBy(value -> {
            StringBuilder sb = new StringBuilder();
            for (Field field : groupByField) {
                String fieldName = field.getFieldName();
                String temp = String.valueOf(value.get(fieldName));
                sb.append(temp);
                sb.append(Constant.SPLIT);
            }

            String result = sb.toString();
            return result.substring(0, result.length() - 1);
        });
    }

    protected GroupedStream<String, ? extends JsonNode> buildHaving(GroupedStream<String, ? extends JsonNode> selectField) {
        if (havingExpression != null) {
            return selectField.filter(value -> {
                try {
                    return havingExpression.isTrue(value);
                } catch (Throwable t) {
                    //使用错误，例如字段是string，使用>过滤；
                    logger.info("having filter error, sql:[{}], value=[{}]", GroupByQueryStatement.this.getContent(), value, t);
                    return false;
                }
            });
        }

        return selectField;
    }
}
