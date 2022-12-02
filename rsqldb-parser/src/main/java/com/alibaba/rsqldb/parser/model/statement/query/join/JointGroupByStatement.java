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
import com.alibaba.rsqldb.parser.model.Field;
import com.alibaba.rsqldb.parser.model.statement.query.phrase.JoinCondition;
import com.alibaba.rsqldb.parser.model.statement.query.phrase.JoinType;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class JointGroupByStatement extends JointStatement {
    private Map<Field, Calculator> operator;

    private List<Field> groupByField;

    public JointGroupByStatement(String sourceTableName, Set<Field> outputField,
                                 JoinType joinType, String asSourceTableName,
                                 String joinTableName, String asJoinTableName,
                                 JoinCondition joinCondition, Map<Field, Calculator> operator, List<Field> groupByField) {
        super(sourceTableName, outputField, joinType, asSourceTableName, joinTableName, asJoinTableName, joinCondition);
        this.operator = operator;
        this.groupByField = groupByField;
    }

    public Map<Field, Calculator> getOperator() {
        return operator;
    }

    public void setOperator(Map<Field, Calculator> operator) {
        this.operator = operator;
    }

    public List<Field> getGroupByField() {
        return groupByField;
    }

    public void setGroupByField(List<Field> groupByField) {
        this.groupByField = groupByField;
    }
}
