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
package com.alibaba.rsqldb.parser.builder;

import com.alibaba.rsqldb.parser.sql.AbstractSqlParser;
import com.alibaba.rsqldb.parser.sql.IParserProvider;
import com.alibaba.rsqldb.parser.sql.ISqlParser;
import com.google.auto.service.AutoService;
import java.util.List;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;

import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.apache.rocketmq.streams.common.model.ServiceName;
import org.apache.rocketmq.streams.configurable.ConfigurableComponent;

@AutoService(IParserProvider.class) @ServiceName("blink") public class FlinkParserProvider implements IParserProvider {
    @Override public ISqlParser createSqlParser(String namespace, String pipelineName, String sql) {
        return createSqlParser(namespace, pipelineName, sql, null);
    }

    @Override public ISqlParser createSqlParser(String namespace, String pipelineName, String sql, ConfigurableComponent component) {
        return new AbstractSqlParser(namespace, pipelineName, sql, component) {
            @Override public List<SqlNode> parseSql(String sql) {
                try {
                    SqlParser.Config parserConfig = SqlParser.config().withParserFactory(FlinkSqlParserImpl.FACTORY).withConformance(FlinkSqlConformance.DEFAULT).withLex(Lex.JAVA).withIdentifierMaxLength(256);
                    SqlParser sqlParser = SqlParser.create(sql, parserConfig);
                    SqlNodeList sqlNodes = sqlParser.parseStmtList();
                    return sqlNodes.getList();
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }
        };
    }

    @Override public String getName() {
        return "blink";
    }
}
