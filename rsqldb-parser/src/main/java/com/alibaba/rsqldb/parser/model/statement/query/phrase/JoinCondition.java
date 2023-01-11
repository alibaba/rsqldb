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

import com.alibaba.rsqldb.common.exception.SyntaxErrorException;
import com.alibaba.rsqldb.parser.model.Field;
import com.alibaba.rsqldb.parser.model.Node;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.antlr.v4.runtime.ParserRuleContext;
import org.apache.rocketmq.streams.core.util.Pair;

import java.util.ArrayList;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class JoinCondition extends Node {
    private List<Pair<Field, Field>> holder = new ArrayList<>();

    @JsonCreator
    public JoinCondition(@JsonProperty("content") String content) {
        super(content);
    }

    public void addField(Field leftField, Field rightField) {
        if (leftField.getTableName() == null || rightField.getTableName() == null) {
            throw new SyntaxErrorException("table name in join condition can not be null.");
        }

        if (leftField.getTableName().equalsIgnoreCase(rightField.getTableName())) {
            throw new SyntaxErrorException("left table name equal to right table in join.");
        }

        Pair<Field, Field> pair = new Pair<>(leftField, rightField);
        this.holder.add(pair);
    }

    public void addJoinCondition(JoinCondition joinCondition) {
        this.holder.addAll(joinCondition.getHolder());
    }


    public List<Pair<Field, Field>> getHolder() {
        return holder;
    }

    public void setHolder(List<Pair<Field, Field>> holder) {
        this.holder = holder;
    }
}
