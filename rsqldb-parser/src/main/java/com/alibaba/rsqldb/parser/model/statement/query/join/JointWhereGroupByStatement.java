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
package com.alibaba.rsqldb.parser.model.statement.query.join;

import com.alibaba.rsqldb.common.exception.SyntaxErrorException;
import com.alibaba.rsqldb.parser.impl.BuildContext;
import com.alibaba.rsqldb.parser.model.Calculator;
import com.alibaba.rsqldb.parser.model.expression.Expression;
import com.alibaba.rsqldb.parser.model.Field;
import com.alibaba.rsqldb.parser.model.statement.query.phrase.JoinCondition;
import com.alibaba.rsqldb.parser.model.statement.query.phrase.JoinType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.rocketmq.streams.core.common.Constant;
import org.apache.rocketmq.streams.core.function.accumulator.Accumulator;
import org.apache.rocketmq.streams.core.rstream.GroupedStream;
import org.apache.rocketmq.streams.core.rstream.RStream;

import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class JointWhereGroupByStatement extends JointWhereStatement {
    private List<Field> groupByField;

    public JointWhereGroupByStatement(String content, String tableName, Map<Field, Calculator> selectFieldAndCalculator, JoinType joinType,
                                      String asSourceTableName, String joinTableName, String asJoinTableName,
                                      JoinCondition joinCondition, Expression expression, boolean before,
                                      List<Field> groupByField) {
        super(content, tableName, selectFieldAndCalculator, joinType, asSourceTableName, joinTableName, asJoinTableName, joinCondition, expression, before);
        if (selectFieldAndCalculator == null || groupByField == null) {
            throw new SyntaxErrorException("not a where groupBy join sql.");
        }
        this.groupByField = groupByField;
    }

    @JsonCreator
    public JointWhereGroupByStatement(@JsonProperty("content") String content, @JsonProperty("tableName") String tableName,
                                      @JsonProperty("selectFieldAndCalculator") Map<Field, Calculator> selectFieldAndCalculator,
                                      @JsonProperty("joinType") JoinType joinType, @JsonProperty("asSourceTableName") String asSourceTableName,
                                      @JsonProperty("joinTableName") String joinTableName, @JsonProperty("asJoinTableName") String asJoinTableName,
                                      @JsonProperty("joinCondition") JoinCondition joinCondition,
                                      @JsonProperty("beforeJoinWhereExpression") Expression beforeJoinWhereExpression,
                                      @JsonProperty("afterJoinWhereExpression") Expression afterJoinWhereExpression,
                                      @JsonProperty("groupByField") List<Field> groupByField) {

        super(content, tableName, selectFieldAndCalculator, joinType, asSourceTableName, joinTableName,
                asJoinTableName, joinCondition, beforeJoinWhereExpression, afterJoinWhereExpression);
        if (selectFieldAndCalculator == null || groupByField == null) {
            throw new SyntaxErrorException("not a where groupBy join sql.");
        }

        this.groupByField = groupByField;
    }

    public List<Field> getGroupByField() {
        return groupByField;
    }

    public void setGroupByField(List<Field> groupByField) {
        this.groupByField = groupByField;
    }

    @Override
    public BuildContext build(BuildContext context) throws Throwable {
        GroupedStream<String, ? extends JsonNode> selectField = buildJoinWhereGBSelect(context);

        context.setGroupedStreamResult(selectField);

        return context;
    }

    protected GroupedStream<String, ? extends JsonNode> buildJoinWhereGBSelect(BuildContext context) {
        RStream<JsonNode> rStream = super.buildJoinWhere(context);

        GroupedStream<String, JsonNode> groupedStream = rStream.keyBy(value -> {
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

        //select
        GroupedStream<String, ? extends JsonNode> selectField = groupedStream;
        if (!isSelectAll()) {
            Accumulator<JsonNode, ObjectNode> select = buildAccumulator();
            selectField = groupedStream.aggregate(select);
        }

        return selectField;
    }
}
