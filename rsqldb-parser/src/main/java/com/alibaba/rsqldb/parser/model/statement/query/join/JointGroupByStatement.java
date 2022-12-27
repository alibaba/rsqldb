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
package com.alibaba.rsqldb.parser.model.statement.query.join;

import com.alibaba.rsqldb.parser.impl.BuildContext;
import com.alibaba.rsqldb.parser.model.Calculator;
import com.alibaba.rsqldb.parser.model.Field;
import com.alibaba.rsqldb.parser.model.statement.query.phrase.JoinCondition;
import com.alibaba.rsqldb.parser.model.statement.query.phrase.JoinType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.antlr.v4.runtime.ParserRuleContext;
import org.apache.rocketmq.streams.core.common.Constant;
import org.apache.rocketmq.streams.core.function.accumulator.Accumulator;
import org.apache.rocketmq.streams.core.rstream.GroupedStream;
import org.apache.rocketmq.streams.core.rstream.RStream;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class JointGroupByStatement extends JointStatement {
    private List<Field> groupByField;

    public JointGroupByStatement(String content, String sourceTableName, Map<Field, Calculator> selectFieldAndCalculator,
                                 JoinType joinType, String asSourceTableName,
                                 String joinTableName, String asJoinTableName,
                                 JoinCondition joinCondition, List<Field> groupByField) {
        super(content, sourceTableName, selectFieldAndCalculator, joinType, asSourceTableName, joinTableName, asJoinTableName, joinCondition);
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
        GroupedStream<String, ? extends JsonNode> groupedStream = buildJoinGBSelect(context);

        context.setGroupedStreamResult(groupedStream);

        return context;
    }

    protected GroupedStream<String, ? extends JsonNode> buildJoinGBSelect(BuildContext context) {
        RStream<JsonNode> leftStream = context.getRStreamSource(this.getTableName());
        RStream<JsonNode> rightStream = context.getRStreamSource(this.getJoinTableName());

        //join
        RStream<JsonNode> rStream = join(leftStream, rightStream);

        //groupby
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
