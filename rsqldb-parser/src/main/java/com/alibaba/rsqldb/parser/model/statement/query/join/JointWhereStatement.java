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

import com.alibaba.rsqldb.parser.impl.BuildContext;
import com.alibaba.rsqldb.parser.model.Calculator;
import com.alibaba.rsqldb.parser.model.Field;
import com.alibaba.rsqldb.parser.model.expression.Expression;
import com.alibaba.rsqldb.parser.model.statement.query.phrase.JoinCondition;
import com.alibaba.rsqldb.parser.model.statement.query.phrase.JoinType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.rocketmq.streams.core.rstream.RStream;

import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class JointWhereStatement extends JointStatement {
    private Expression beforeJoinWhereExpression;
    private Expression afterJoinWhereExpression;

    public JointWhereStatement(String content, String tableName,
                               Map<Field, Calculator> selectFieldAndCalculator,
                               JoinType joinType, String asSourceTableName,
                               String joinTableName, String asJoinTableName,
                               JoinCondition joinCondition, Expression expression,
                               boolean before) {
        super(content, tableName, selectFieldAndCalculator, joinType, asSourceTableName, joinTableName, asJoinTableName, joinCondition);
        if (expression == null) {
            throw new IllegalArgumentException("expression can not be null");
        }

        if (before) {
            this.beforeJoinWhereExpression = expression;
        } else {
            this.afterJoinWhereExpression = expression;
        }
    }

    @JsonCreator
    public JointWhereStatement(@JsonProperty("content") String content, @JsonProperty("tableName") String tableName,
                               @JsonProperty("selectFieldAndCalculator") Map<Field, Calculator> selectFieldAndCalculator,
                               @JsonProperty("joinType") JoinType joinType, @JsonProperty("asSourceTableName") String asSourceTableName,
                               @JsonProperty("joinTableName") String joinTableName, @JsonProperty("asJoinTableName") String asJoinTableName,
                               @JsonProperty("joinCondition") JoinCondition joinCondition,
                               @JsonProperty("beforeJoinWhereExpression") Expression beforeJoinWhereExpression,
                               @JsonProperty("afterJoinWhereExpression") Expression afterJoinWhereExpression) {

        super(content, tableName, selectFieldAndCalculator, joinType, asSourceTableName, joinTableName, asJoinTableName, joinCondition);
        if (beforeJoinWhereExpression == null && afterJoinWhereExpression == null) {
            throw new IllegalArgumentException("expression can not be null");
        }
        this.beforeJoinWhereExpression = beforeJoinWhereExpression;
        this.afterJoinWhereExpression = afterJoinWhereExpression;
    }

    public Expression getBeforeJoinWhereExpression() {
        return beforeJoinWhereExpression;
    }

    public void setBeforeJoinWhereExpression(Expression beforeJoinWhereExpression) {
        this.beforeJoinWhereExpression = beforeJoinWhereExpression;
    }

    public Expression getAfterJoinWhereExpression() {
        return afterJoinWhereExpression;
    }

    public void setAfterJoinWhereExpression(Expression afterJoinWhereExpression) {
        this.afterJoinWhereExpression = afterJoinWhereExpression;
    }

    @Override
    public BuildContext build(BuildContext context) throws Throwable {
        RStream<JsonNode> rStream = buildJoinWhere(context);

        //select
        buildSelectItem(rStream, context);

        return context;
    }

    protected RStream<JsonNode> buildJoinWhere(BuildContext context) {
        //before where
        RStream<JsonNode> leftStream = context.getRStreamSource(this.getTableName());
        if (beforeJoinWhereExpression != null) {
            leftStream = leftStream.filter(value -> beforeJoinWhereExpression.isTrue(value));
        }

        RStream<JsonNode> rightStream = context.getRStreamSource(this.getJoinTableName());

        //join
        RStream<JsonNode> rStream = join(leftStream, rightStream);

        //after where
        return rStream.filter(value -> afterJoinWhereExpression.isTrue(value));
    }
}
