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

import com.alibaba.rsqldb.common.exception.SyntaxErrorException;
import com.alibaba.rsqldb.parser.impl.BuildContext;
import com.alibaba.rsqldb.parser.model.Calculator;
import com.alibaba.rsqldb.parser.model.Field;
import com.alibaba.rsqldb.parser.model.expression.Expression;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.rocketmq.streams.core.rstream.GroupedStream;
import org.apache.rocketmq.streams.core.rstream.RStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class FilterQueryStatement extends QueryStatement {
    private static final Logger logger = LoggerFactory.getLogger(FilterQueryStatement.class);
    private Expression filter;

    public FilterQueryStatement(String content, String sourceTableName, Map<Field, Calculator> outputFieldAndCalculator, Expression filter) {
        super(content, sourceTableName, outputFieldAndCalculator);
        for (Calculator value : outputFieldAndCalculator.values()) {
            if (value != null) {
                throw new SyntaxErrorException("has function in sql. function=" + value + ", sql=" + content);
            }
        }
        this.filter = filter;
    }

    public Expression getFilter() {
        return filter;
    }

    public void setFilter(Expression filter) {
        this.filter = filter;
    }

    @Override
    public BuildContext build(BuildContext context) throws Throwable {

        //先where过滤在select 过滤
        RStream<JsonNode> rStream = context.getrStream();

        rStream = rStream.filter(value -> {
            try {
                return filter.isTrue(value);
            } catch (Throwable t) {
                //使用错误，例如字段是string，使用>过滤；
                logger.info("filter error, sql:[{}], value=[{}]", FilterQueryStatement.this.getContent(), value, t);
                return false;
            }
        });

        context.setrStream(rStream);

        //select 过滤
        return super.build(context);
    }


}
