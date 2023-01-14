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
import com.alibaba.rsqldb.parser.model.statement.query.join.JointStatement;
import com.alibaba.rsqldb.rest.service.RSQLConfig;
import org.apache.rocketmq.common.message.Message;
import org.springframework.stereotype.Service;

import java.io.SyncFailedException;
import java.util.function.Function;

@Service
public class TaskFactory {
    private RSQLConfig rsqlConfig;
    private Function<String, Statement> function;

    public TaskFactory(RSQLConfig rsqlConfig) {
        this.rsqlConfig = rsqlConfig;
    }

    public void init(Function<String, Statement> function) {
        this.function = function;
    }

    public BuildContext dispatch(Statement statement, BuildContext context) throws Throwable {
        if (statement instanceof CreateTableStatement || statement instanceof CreateViewStatement) {
            //no-op
            // 不执行，由其他sql语句触发，比如insert into ... from viewTable.
            return null;
        }

        String tableName = statement.getTableName();

        if (statement instanceof InsertValueStatement) {
            //查询insert表是否存在
            Statement temp = function.apply(tableName);
            if (!(temp instanceof CreateTableStatement)) {
                throw new SyncFailedException("insert table not exist.");
            }

            CreateTableStatement createTableStatement = (CreateTableStatement) temp;

            context.setCreateTableStatement(createTableStatement);
            //没有source表，只有sink，不能使用streams插入，直接使用producer写入数据
            this.build((InsertValueStatement) statement, context, createTableStatement.getTopicName());

            return null;
        } else if (statement instanceof InsertQueryStatement) {
            InsertQueryStatement insertQueryStatement = (InsertQueryStatement) statement;
            QueryStatement queryStatement = insertQueryStatement.getQueryStatement();
            String sourceTableName = queryStatement.getTableName();
            context = prepare(sourceTableName, context, RSQLConstant.TableType.SOURCE);

            context = this.build((InsertQueryStatement) statement, context);

            context = prepare(tableName, context, RSQLConstant.TableType.SINK);
        } else if (statement instanceof QueryStatement) {
            context = prepare(tableName, context, RSQLConstant.TableType.SOURCE);

            context = build((QueryStatement) statement, context);

            //todo 没有输出目的地的select（来自CLI命令行，输出到返回中），如果即没有response也米有其他返回，不能直接执行。
            context = prepare(tableName, context, RSQLConstant.TableType.SINK);
        } else {
            throw new RSQLServerException("unknown statement type=" + statement.getClass() + ".sql=" + statement.getContent());
        }

        return context;
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
        } else if (queryStatement instanceof JointStatement ) {
            JointStatement jointStatement = (JointStatement) queryStatement;

            String joinTableName = jointStatement.getJoinTableName();
            prepare(joinTableName, context, RSQLConstant.TableType.SOURCE);

            jointStatement.build(context);
        }

        return context;
    }

    private BuildContext prepare(String tableName, BuildContext context, RSQLConstant.TableType type) throws Throwable {
        Statement statement = function.apply(tableName);

        context.putHeader(RSQLConstant.TABLE_TYPE, type);
        return statement.build(context);
    }


}
