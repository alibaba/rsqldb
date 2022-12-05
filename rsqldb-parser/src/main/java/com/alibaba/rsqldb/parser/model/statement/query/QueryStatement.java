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
import com.alibaba.rsqldb.parser.model.Field;
import com.alibaba.rsqldb.parser.model.Node;
import org.antlr.v4.runtime.ParserRuleContext;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class QueryStatement extends Node {
    private String sourceTableName;

    private Map<Field/*输出的字段*/, Calculator/*针对字段的计算方式，可能为null*/> selectFieldAndCalculator;

//    public QueryStatement(ParserRuleContext context, String sourceTableName, Set<Field> outputField) {
//        super(context);
//        this.sourceTableName = sourceTableName;
//        this.outputField = outputField;
//    }


    public QueryStatement(ParserRuleContext context, String sourceTableName, Map<Field, Calculator> selectFieldAndCalculator) {
        super(context);
        this.sourceTableName = sourceTableName;
        this.selectFieldAndCalculator = selectFieldAndCalculator;
    }

    public String getSourceTableName() {
        return sourceTableName;
    }

    public void setSourceTableName(String sourceTableName) {
        this.sourceTableName = sourceTableName;
    }

    public Map<Field, Calculator> getSelectFieldAndCalculator() {
        return selectFieldAndCalculator;
    }

    public void setSelectFieldAndCalculator(Map<Field, Calculator> selectFieldAndCalculator) {
        this.selectFieldAndCalculator = selectFieldAndCalculator;
    }

//    public Set<Field> getOutputField() {
//        return outputField;
//    }
//
//    public void setOutputField(Set<Field> outputField) {
//        this.outputField = outputField;
//    }

}
