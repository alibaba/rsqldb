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
package com.alibaba.rsqldb.parser.model.statement;

import com.alibaba.rsqldb.parser.impl.BuildContext;
import com.alibaba.rsqldb.parser.model.statement.query.QueryStatement;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class CreateViewStatement extends Statement {
    private QueryStatement queryStatement;

    @JsonCreator
    public CreateViewStatement(@JsonProperty("content") String content, @JsonProperty("tableName") String tableName,
                               @JsonProperty("queryStatement") QueryStatement queryStatement) {
        super(content, tableName);
        this.queryStatement = queryStatement;
    }

    public QueryStatement getQueryStatement() {
        return queryStatement;
    }

    public void setQueryStatement(QueryStatement queryStatement) {
        this.queryStatement = queryStatement;
    }

    @Override
    public BuildContext build(BuildContext context) throws Throwable {
        return queryStatement.build(context);
    }

    @Override
    public String toString() {
        return "CreateViewStatement{" +
                "queryStatement=" + queryStatement +
                '}';
    }
}
