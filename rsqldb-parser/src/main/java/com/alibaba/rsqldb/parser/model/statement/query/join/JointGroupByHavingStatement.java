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
package com.alibaba.rsqldb.parser.model.statement.query.join;

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
import org.antlr.v4.runtime.ParserRuleContext;
import org.apache.rocketmq.streams.core.rstream.GroupedStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class JointGroupByHavingStatement extends JointGroupByStatement {
    private static final Logger logger = LoggerFactory.getLogger(JointGroupByHavingStatement.class);
    private Expression havingExpression;

    @JsonCreator
    public JointGroupByHavingStatement(@JsonProperty("content") String content, @JsonProperty("tableName") String tableName,
                                       @JsonProperty("selectFieldAndCalculator") Map<Field, Calculator> selectFieldAndCalculator,
                                       @JsonProperty("joinType") JoinType joinType, @JsonProperty("asSourceTableName") String asSourceTableName,
                                       @JsonProperty("joinTableName") String joinTableName, @JsonProperty("asJoinTableName") String asJoinTableName,
                                       @JsonProperty("joinCondition") JoinCondition joinCondition, @JsonProperty("groupByField") List<Field> groupByField,
                                       @JsonProperty("havingExpression") Expression havingExpression) {
        super(content, tableName, selectFieldAndCalculator, joinType, asSourceTableName, joinTableName, asJoinTableName, joinCondition, groupByField);
        super.validAndPrePareHavingExpression(havingExpression);
        this.havingExpression = havingExpression;
    }

    public Expression getHavingExpression() {
        return havingExpression;
    }

    public void setHavingExpression(Expression havingExpression) {
        this.havingExpression = havingExpression;
    }

    @Override
    public BuildContext build(BuildContext context) throws Throwable {
        GroupedStream<String, ? extends JsonNode> groupedStream = buildJoinGBSelect(context);
        groupedStream = groupedStream.filter(value -> {
            try {
                return havingExpression.isTrue(value);
            } catch (Throwable t) {
                //使用错误，例如字段是string，使用>过滤；
                logger.info("having filter error, sql:[{}], value=[{}]", JointGroupByHavingStatement.this.getContent(), value, t);
                return false;
            }
        });

        context.setGroupedStreamResult(groupedStream);

        return context;
    }
}
