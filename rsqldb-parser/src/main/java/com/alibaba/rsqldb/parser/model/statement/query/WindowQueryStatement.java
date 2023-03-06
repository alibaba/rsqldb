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
package com.alibaba.rsqldb.parser.model.statement.query;

import com.alibaba.rsqldb.common.RSQLConstant;
import com.alibaba.rsqldb.common.exception.SyntaxErrorException;
import com.alibaba.rsqldb.parser.impl.BuildContext;
import com.alibaba.rsqldb.parser.model.Calculator;
import com.alibaba.rsqldb.parser.model.expression.Expression;
import com.alibaba.rsqldb.parser.model.Field;
import com.alibaba.rsqldb.parser.model.statement.query.phrase.ExpressionType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.rocketmq.streams.core.function.accumulator.Accumulator;
import org.apache.rocketmq.streams.core.metadata.StreamConfig;
import org.apache.rocketmq.streams.core.rstream.GroupedStream;
import org.apache.rocketmq.streams.core.rstream.RStream;
import org.apache.rocketmq.streams.core.rstream.WindowStream;
import org.apache.rocketmq.streams.core.window.Time;
import org.apache.rocketmq.streams.core.window.WindowBuilder;
import org.apache.rocketmq.streams.core.window.WindowInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class WindowQueryStatement extends GroupByQueryStatement {
    private static final Logger logger = LoggerFactory.getLogger(WindowQueryStatement.class);

    private WindowInfoInSQL groupByWindow;

    public WindowQueryStatement(String content, String tableName,
                                Map<Field, Calculator> selectFieldAndCalculator, List<Field> groupByField,
                                WindowInfoInSQL groupByWindow) {
        super(content, tableName, selectFieldAndCalculator, groupByField);
        this.groupByWindow = groupByWindow;
        validator();
    }

    public WindowQueryStatement(String content, String tableName,
                                Map<Field, Calculator> selectFieldAndCalculator, List<Field> groupByField,
                                WindowInfoInSQL groupByWindow, Expression filter, ExpressionType expressionType) {
        super(content, tableName, selectFieldAndCalculator, groupByField, filter, expressionType);
        this.groupByWindow = groupByWindow;
        validator();
    }

    @JsonCreator
    public WindowQueryStatement(@JsonProperty("content") String content, @JsonProperty("tableName") String tableName,
                                @JsonProperty("selectFieldAndCalculator") Map<Field, Calculator> selectFieldAndCalculator,
                                @JsonProperty("groupByField") List<Field> groupByField,
                                @JsonProperty("groupByWindow") WindowInfoInSQL groupByWindow,
                                @JsonProperty("whereExpression") Expression whereExpression, @JsonProperty("havingExpression") Expression havingExpression) {
        super(content, tableName, selectFieldAndCalculator, groupByField, whereExpression, havingExpression);
        this.groupByWindow = groupByWindow;
        validator();
    }

    public WindowInfoInSQL getGroupByWindow() {
        return groupByWindow;
    }

    public void setGroupByWindow(WindowInfoInSQL groupByWindow) {
        this.groupByWindow = groupByWindow;
    }

    //groupBy后面除了window，还必须对某些字段进行聚合；
    private void validator() {
        if (this.getGroupByField() == null || this.getGroupByField().size() == 0) {
            throw new SyntaxErrorException("Must has a groupBy field in window sql. sql=" + this.getContent());
        }
    }

    @Override
    public BuildContext build(BuildContext context) throws Throwable {
        context.putHeader(RSQLConstant.CONFIG_PREFIX + StreamConfig.ALLOW_LATENESS_MILLISECOND, 10*1000);

        RStream<JsonNode> rStream = context.getRStreamSource(this.getTableName());
        RStream<JsonNode> stream = rStream.selectTimestamp(value -> {
            String timeField = groupByWindow.getTimeField().getFieldName();
            JsonNode node = value.get(timeField);
            try {
                return node.asLong();
            } catch (Throwable t) {
                logger.error("get time from value error, time field :[{}], value=[{}]", timeField, value);
                throw t;
            }
        });


        GroupedStream<String, JsonNode> groupedStream = buildGroupBy(stream);

        WindowInfo windowInfo;
        switch (groupByWindow.getType()) {
            case TUMBLE: {
                windowInfo = WindowBuilder.tumblingWindow(Time.of(groupByWindow.getSize(), groupByWindow.getTimeUnit()));
                break;
            }
            case HOP: {
                windowInfo = WindowBuilder.slidingWindow(Time.of(groupByWindow.getSize(), groupByWindow.getTimeUnit()),
                        Time.of(groupByWindow.getSlide(), groupByWindow.getTimeUnit()));
                break;
            }
            case SESSION: {
                windowInfo = WindowBuilder.sessionWindow(Time.of(groupByWindow.getSize(), groupByWindow.getTimeUnit()));
                break;
            }
            default: {
                throw new IllegalArgumentException("unknown window type: " + groupByWindow.getType());
            }
        }

        WindowStream<String, JsonNode> windowStream = groupedStream.window(windowInfo);

        WindowStream<String, ? extends JsonNode> selectField = windowStream;
        //select
        if (!isSelectAll()) {
            Accumulator<JsonNode, ObjectNode> action = buildAccumulator();
            selectField = windowStream.aggregate(action);
        }

        //having
        selectField = buildHaving(selectField);


        context.setWindowStreamResult(selectField);

        return context;
    }

    protected WindowStream<String, ? extends JsonNode> buildHaving(WindowStream<String, ? extends JsonNode> selectField) {
        if (this.getHavingExpression() != null) {
            return selectField.filter(value -> {
                try {
                    return this.getHavingExpression().isTrue(value);
                } catch (Throwable t) {
                    //使用错误，例如字段是string，使用>过滤；
                    logger.warn("having filter error, sql:[{}], value=[{}]", WindowQueryStatement.this.getContent(), value, t);
                    return false;
                }
            });
        }

        return selectField;
    }
}
