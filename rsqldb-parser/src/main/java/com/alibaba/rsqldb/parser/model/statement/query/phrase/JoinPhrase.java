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
package com.alibaba.rsqldb.parser.model.statement.query.phrase;

import com.alibaba.rsqldb.parser.model.Node;
import org.antlr.v4.runtime.ParserRuleContext;

public class JoinPhrase extends Node {
    private JoinType joinType;
    private String joinTableName;
    //可能为空
    private String asJoinTableName;
    private JoinCondition joinCondition;

    public JoinPhrase(String content, JoinType joinType, String joinTableName, String asJoinTableName, JoinCondition joinCondition) {
        super(content);
        this.joinType = joinType;
        this.joinTableName = joinTableName;
        this.asJoinTableName = asJoinTableName;
        this.joinCondition = joinCondition;
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
}
