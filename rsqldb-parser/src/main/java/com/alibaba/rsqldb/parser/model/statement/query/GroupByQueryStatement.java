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

import com.alibaba.rsqldb.parser.model.Calculator;
import com.alibaba.rsqldb.parser.model.expression.Expression;
import com.alibaba.rsqldb.parser.model.Field;
import com.alibaba.rsqldb.parser.model.expression.SingleValueCalcuExpression;
import org.antlr.v4.runtime.ParserRuleContext;

import java.util.List;
import java.util.Map;
import java.util.Set;

//聚合查询
public class GroupByQueryStatement extends QueryStatement {
    private Expression whereExpression;
    private Expression havingExpression;


    private Map<Field, Calculator> operator;

    private List<Field> groupByField;

    public GroupByQueryStatement(ParserRuleContext context, String sourceTableName, Set<Field> outputField, Map<Field, Calculator> operator, List<Field> groupByField) {
        super(context, sourceTableName, outputField);
        this.operator = operator;
        this.groupByField = groupByField;
    }

    public GroupByQueryStatement(ParserRuleContext context, String sourceTableName, Set<Field> outputField, Map<Field, Calculator> operator,
                                 List<Field> groupByField, Expression expression) {

        super(context, sourceTableName, outputField);
        if (expression instanceof SingleValueCalcuExpression) {
            this.havingExpression = expression;
        } else {
            this.whereExpression = expression;
        }
        this.operator = operator;
        this.groupByField = groupByField;
    }

    public GroupByQueryStatement(ParserRuleContext context, String sourceTableName, Set<Field> outputField, Map<Field, Calculator> operator,
                                 List<Field> groupByField, Expression whereExpression, Expression havingExpression) {

        super(context, sourceTableName, outputField);
        assert !(whereExpression instanceof SingleValueCalcuExpression);
        assert havingExpression instanceof SingleValueCalcuExpression;
        this.whereExpression = whereExpression;
        this.havingExpression = havingExpression;
        this.operator = operator;
        this.groupByField = groupByField;
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

    public Map<Field, Calculator> getOperator() {
        return operator;
    }

    public void setOperator(Map<Field, Calculator> operator) {
        this.operator = operator;
    }
}
