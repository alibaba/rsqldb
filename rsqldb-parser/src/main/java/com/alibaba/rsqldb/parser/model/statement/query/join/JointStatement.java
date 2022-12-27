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
import com.alibaba.rsqldb.parser.model.statement.query.QueryStatement;
import com.alibaba.rsqldb.parser.model.statement.query.phrase.JoinCondition;
import com.alibaba.rsqldb.parser.model.statement.query.phrase.JoinType;

import java.util.Map;

public class JointStatement extends QueryStatement {
    private JoinType joinType;
    private String asSourceTableName;
    private String joinTableName;
    private String asJoinTableName;

    private JoinCondition joinCondition;

    public JointStatement(String content, String sourceTableName, Map<Field, Calculator> selectFieldAndCalculator,
                          JoinType joinType, String asSourceTableName,
                          String joinTableName, String asJoinTableName, JoinCondition joinCondition) {
        super(content, sourceTableName, selectFieldAndCalculator);
        this.joinType = joinType;
        this.asSourceTableName = asSourceTableName;
        this.joinTableName = joinTableName;
        this.asJoinTableName = asJoinTableName;
        this.joinCondition = joinCondition;
    }

    public String getAsSourceTableName() {
        return asSourceTableName;
    }

    public void setAsSourceTableName(String asSourceTableName) {
        this.asSourceTableName = asSourceTableName;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public void setJoinType(JoinType joinType) {
        this.joinType = joinType;
    }

    public String getJoinTableName() {
        return joinTableName;
    }

    public void setJoinTableName(String joinTableName) {
        this.joinTableName = joinTableName;
    }

    public String getAsJoinTableName() {
        return asJoinTableName;
    }

    public void setAsJoinTableName(String asJoinTableName) {
        this.asJoinTableName = asJoinTableName;
    }

    public JoinCondition getJoinCondition() {
        return joinCondition;
    }

    public void setJoinCondition(JoinCondition joinCondition) {
        this.joinCondition = joinCondition;
    }

    @Override
    public BuildContext build(BuildContext context) throws Throwable {

        return null;
    }
}
