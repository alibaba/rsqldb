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
import org.antlr.v4.runtime.ParserRuleContext;

import java.util.Map;
import java.util.Set;

public class FilterQueryStatement extends QueryStatement {
    private Expression filter;

    public FilterQueryStatement(ParserRuleContext context, String sourceTableName, Map<Field, Calculator> outputFieldAndCalculator, Expression filter) {
        super(context, sourceTableName, outputFieldAndCalculator);
        this.filter = filter;
    }

    public Expression getFilter() {
        return filter;
    }

    public void setFilter(Expression filter) {
        this.filter = filter;
    }
}