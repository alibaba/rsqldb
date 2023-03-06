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
package com.alibaba.rsqldb.parser.model.statement.query.phrase;

import com.alibaba.rsqldb.common.exception.SyntaxErrorException;
import com.alibaba.rsqldb.parser.model.Node;
import com.alibaba.rsqldb.parser.model.expression.Expression;
import org.antlr.v4.runtime.ParserRuleContext;

public class HavingPhrase extends Node {
    private Expression havingExpression;

    public HavingPhrase(String content, Expression havingExpression) {
        super(content);
        if (havingExpression == null) {
            throw new SyntaxErrorException("having is null.");
        }
        this.havingExpression = havingExpression;
    }

    public Expression getHavingExpression() {
        return havingExpression;
    }

    public void setHavingExpression(Expression havingExpression) {
        this.havingExpression = havingExpression;
    }
}
