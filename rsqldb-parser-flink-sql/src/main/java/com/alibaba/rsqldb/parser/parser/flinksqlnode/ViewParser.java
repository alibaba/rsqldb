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
package com.alibaba.rsqldb.parser.parser.flinksqlnode;


import com.alibaba.rsqldb.parser.parser.SqlNodeParserFactory;
import com.alibaba.rsqldb.parser.parser.builder.AbstractSqlBuilder;
import com.alibaba.rsqldb.parser.parser.builder.SelectSqlBuilder;
import com.alibaba.rsqldb.parser.parser.builder.ViewSqlBuilder;
import com.alibaba.rsqldb.parser.parser.result.BuilderParseResult;
import com.alibaba.rsqldb.parser.parser.result.IParseResult;
import com.alibaba.rsqldb.parser.parser.sqlnode.AbstractSqlNodeNodeParser;
import java.util.Set;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.sql.parser.ddl.SqlCreateView;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

public class ViewParser extends AbstractSqlNodeNodeParser<SqlCreateView, ViewSqlBuilder> {

    @Override
    public IParseResult parse(ViewSqlBuilder viewSQLBuilder, SqlCreateView sqlCreateView) {
        String createViewName= FunctionUtils.getConstant(sqlCreateView.getViewName().toString());
        viewSQLBuilder.setTableName(createViewName);
        viewSQLBuilder.setSqlNode(sqlCreateView);
        SqlNode sqlNode = sqlCreateView.getQuery();
        AbstractSqlBuilder builder = SqlNodeParserFactory.parseBuilder(sqlNode);
        ////保存所有create对应的builder， 在insert或维表join时使用
        builder.setTableName2Builders(viewSQLBuilder.getTableName2Builders());
        viewSQLBuilder.setBuilder(builder);
        viewSQLBuilder.addCreatedTable(viewSQLBuilder.getTableName());
        if (builder instanceof SelectSqlBuilder) {
            /**
             * 主要是处理*的场景，把*转化成具体的字段值
             */
            SelectSqlBuilder selectSQLBuilder = (SelectSqlBuilder) builder;
            Set<String> fieldNames = selectSQLBuilder.getAllFieldNames();
            viewSQLBuilder.setFieldNames(fieldNames);
        }


        return new BuilderParseResult(viewSQLBuilder);
    }

    @Override
    public boolean support(Object sqlNode) {
        return sqlNode instanceof SqlCreateView;
    }

    @Override
    public ViewSqlBuilder create() {
        return new ViewSqlBuilder();
    }
}
