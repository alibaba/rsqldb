 /*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
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
package com.alibaba.rsqldb.parser.model.statement.query;

import com.alibaba.rsqldb.parser.impl.BuildContext;
import com.alibaba.rsqldb.parser.model.Calculator;
import com.alibaba.rsqldb.parser.model.Field;
import com.alibaba.rsqldb.parser.model.expression.Expression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.rocketmq.streams.core.rstream.RStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class FilterQueryStatement extends QueryStatement {
    private static final Logger logger = LoggerFactory.getLogger(FilterQueryStatement.class);
    private Expression filter;

    @JsonCreator
    public FilterQueryStatement(@JsonProperty("content") String content, @JsonProperty("tableName") String tableName,
                                @JsonProperty("selectFieldAndCalculator") Map<Field, Calculator> selectFieldAndCalculator,
                                @JsonProperty("filter") Expression filter) {
        super(content, tableName, selectFieldAndCalculator);
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
        RStream<JsonNode> rStream = context.getRStreamSource(this.getTableName());

        rStream = rStream.filter(value -> {
            try {
                return filter.isTrue(value);
            } catch (Throwable t) {
                //使用错误，例如字段是string，使用>过滤；
                logger.info("filter error, sql:[{}], value=[{}]", FilterQueryStatement.this.getContent(), value, t);
                return false;
            }
        });

        //select 过滤
        buildSelectItem(rStream, context);

        return context;
    }


}
