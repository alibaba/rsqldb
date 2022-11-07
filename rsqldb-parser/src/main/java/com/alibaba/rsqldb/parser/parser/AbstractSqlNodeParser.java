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
package com.alibaba.rsqldb.parser.parser;

import com.alibaba.rsqldb.parser.parser.builder.AbstractSqlBuilder;
import com.alibaba.rsqldb.parser.parser.builder.SelectSqlBuilder;
import com.alibaba.rsqldb.parser.parser.result.IParseResult;
import com.alibaba.rsqldb.parser.parser.result.NotSupportParseResult;
import com.alibaba.rsqldb.parser.parser.result.ScriptParseResult;
import java.util.HashSet;
import java.util.Set;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class AbstractSqlNodeParser<T, D extends AbstractSqlBuilder> implements ISqlNodeParser<T, D> {

    private static final Log LOG = LogFactory.getLog(AbstractSqlNodeParser.class);

    protected IParseResult parseSqlNode(D builder, SqlNode sqlNode) {
        try {
            ISqlNodeParser sqlParser = SqlNodeParserFactory.getParse(sqlNode);
            IParseResult parseResult = sqlParser.parse(builder, sqlNode);
            if (builder instanceof SelectSqlBuilder) {
                SelectSqlBuilder sqlBuilder = (SelectSqlBuilder) builder;
                if (sqlBuilder.isWhereStage() && parseResult instanceof ScriptParseResult) {
                    ScriptParseResult scriptParseResult = (ScriptParseResult) parseResult;
                    Set<String> scripts = new HashSet<>();
                    scripts.addAll(scriptParseResult.getScriptValueList());
                    scripts.addAll(sqlBuilder.getScripts());
                    if (scripts != null) {
                        boolean isExpressionScript = true;
                        for (String script : scripts) {
                            if (script.startsWith("(") && script.endsWith(")")) {
                                isExpressionScript = false;
                                break;
                            }

                        }
                        if (isExpressionScript) {
                            if (!sqlBuilder.getExpressionFunctionSql().contains(sqlNode.toString())) {
                                sqlBuilder.getExpressionFunctionSql().add(sqlNode.toString());
                            }

                        }

                    }

                }
            }
            return parseResult;
        } catch (NullPointerException e) {
            builder.setSupportOptimization(false);
            LOG.error("can not parser sql node " + sqlNode.toString(), e);
            return new NotSupportParseResult(sqlNode);
        }

    }

}
