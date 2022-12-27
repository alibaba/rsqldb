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
package com.alibaba.rsqldb.rest.service.iml;

import com.alibaba.rsqldb.common.RSQLConstant;
import com.alibaba.rsqldb.common.exception.RSQLServerException;
import com.alibaba.rsqldb.parser.impl.BuildContext;
import com.alibaba.rsqldb.parser.model.statement.CreateTableStatement;
import com.alibaba.rsqldb.parser.model.statement.CreateViewStatement;
import com.alibaba.rsqldb.parser.model.statement.InsertQueryStatement;
import com.alibaba.rsqldb.parser.model.statement.InsertValueStatement;
import com.alibaba.rsqldb.parser.model.statement.Statement;
import com.alibaba.rsqldb.parser.model.statement.query.FilterQueryStatement;
import com.alibaba.rsqldb.parser.model.statement.query.GroupByQueryStatement;
import com.alibaba.rsqldb.parser.model.statement.query.QueryStatement;
import com.alibaba.rsqldb.parser.model.statement.query.WindowQueryStatement;
import com.alibaba.rsqldb.rest.service.RSQLConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.rocketmq.common.message.Message;
import org.springframework.stereotype.Service;

import java.util.function.Function;

@Service
public class TaskFactory {
    private RSQLConfig rsqlConfig;
    private ObjectMapper objectMapper = new ObjectMapper();

    public TaskFactory(RSQLConfig rsqlConfig) {
        this.rsqlConfig = rsqlConfig;
    }

    public void dispatch(Statement statement, Function<String, CreateTableStatement> function, BuildContext context) throws Throwable {
        String tableName = statement.getTableName();

        if (statement instanceof InsertValueStatement) {
            //查询insert表是否存在
            CreateTableStatement createTableStatement = function.apply(tableName);
            context.setCreateTableStatement(createTableStatement);
            //没有source表，只有sink，不能使用streams插入，直接使用producer写入数据
            this.build((InsertValueStatement) statement, context, createTableStatement.getTopicName());
        } else if (statement instanceof InsertQueryStatement) {
            InsertQueryStatement insertQueryStatement = (InsertQueryStatement) statement;
            QueryStatement queryStatement = insertQueryStatement.getQueryStatement();
            tableName = queryStatement.getTableName();
            context = prepare(tableName, function, context, RSQLConstant.TableType.SOURCE);

            context = this.build(insertQueryStatement, context);

            context = prepare(tableName, function, context, RSQLConstant.TableType.SINK);
        } else if (statement instanceof QueryStatement) {
            context = prepare(tableName, function, context, RSQLConstant.TableType.SOURCE);
            context = build((QueryStatement) statement, context);


        }


    }

    public void build(CreateTableStatement createTableStatement, BuildContext context) throws Exception {
    }

    public void build(CreateViewStatement createViewStatement, BuildContext context) throws Exception {
    }

    private BuildContext build(InsertQueryStatement insertQueryStatement, BuildContext context) throws Throwable {
        context = this.build(insertQueryStatement.getQueryStatement(), context);

        return insertQueryStatement.build(context);
    }

    private void build(InsertValueStatement insertValueStatement, BuildContext context, String topicName) throws Throwable {
        context = insertValueStatement.build(context);

        Message message = new Message(topicName, context.getInsertValueData());
        context.getProducer().send(message);
    }

    private BuildContext build(QueryStatement queryStatement, BuildContext context) throws Throwable {
        if (queryStatement.getClass().getName().equals(QueryStatement.class.getName())) {
            context = queryStatement.build(context);
        }else if (queryStatement instanceof FilterQueryStatement) {
            context = ((FilterQueryStatement)queryStatement).build(context);
        } else if (queryStatement.getClass().getName().equals(GroupByQueryStatement.class.getName())) {
            GroupByQueryStatement groupByQueryStatement = (GroupByQueryStatement) queryStatement;
            context = groupByQueryStatement.build(context);
        } else if (queryStatement instanceof WindowQueryStatement) {
            WindowQueryStatement windowQueryStatement = (WindowQueryStatement) queryStatement;
            windowQueryStatement.build(context);
        }

        return context;
    }

    private BuildContext prepare(String tableName, Function<String, CreateTableStatement> function,
                                 BuildContext context, RSQLConstant.TableType type) throws Throwable {
        CreateTableStatement createTableStatement = function.apply(tableName);
        if (createTableStatement == null) {
            throw new RSQLServerException("createTableStatement is null query by table name:[" + tableName + "]");
        }

        context.putHeader(RSQLConstant.TABLE_TYPE, type);
        return createTableStatement.build(context);
    }


}
