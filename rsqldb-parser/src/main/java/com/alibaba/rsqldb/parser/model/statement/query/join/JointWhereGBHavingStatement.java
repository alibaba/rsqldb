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

import com.alibaba.rsqldb.parser.model.Calculator;
import com.alibaba.rsqldb.parser.model.expression.Expression;
import com.alibaba.rsqldb.parser.model.Field;
import com.alibaba.rsqldb.parser.model.statement.query.phrase.JoinCondition;
import com.alibaba.rsqldb.parser.model.statement.query.phrase.JoinType;
import org.antlr.v4.runtime.ParserRuleContext;

import java.util.List;
import java.util.Map;

public class JointWhereGBHavingStatement extends JointWhereGroupByStatement {
    private Expression havingExpression;

    public JointWhereGBHavingStatement(ParserRuleContext context, String sourceTableName, Map<Field, Calculator> selectFieldAndCalculator, JoinType joinType,
                                       String asSourceTableName, String joinTableName, String asJoinTableName,
                                       JoinCondition joinCondition, Expression expression, boolean before,
                                       List<Field> groupByField, Expression havingExpression) {
        super(context, sourceTableName, selectFieldAndCalculator, joinType, asSourceTableName, joinTableName, asJoinTableName,
                joinCondition, expression, before, groupByField);
        this.havingExpression = havingExpression;
    }

    public JointWhereGBHavingStatement(ParserRuleContext context, String sourceTableName, Map<Field, Calculator> selectFieldAndCalculator, JoinType joinType,
                                       String asSourceTableName, String joinTableName, String asJoinTableName,
                                       JoinCondition joinCondition, Expression beforeJoinWhereExpression, Expression afterJoinWhereExpression,
                                       List<Field> groupByField, Expression havingExpression) {
        super(context, sourceTableName, selectFieldAndCalculator, joinType, asSourceTableName, joinTableName, asJoinTableName,
                joinCondition, beforeJoinWhereExpression, afterJoinWhereExpression, groupByField);
        this.havingExpression = havingExpression;
    }

    public Expression getHavingExpression() {
        return havingExpression;
    }

    public void setHavingExpression(Expression havingExpression) {
        this.havingExpression = havingExpression;
    }
}
