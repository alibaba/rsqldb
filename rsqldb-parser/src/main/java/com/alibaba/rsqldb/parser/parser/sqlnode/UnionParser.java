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
package com.alibaba.rsqldb.parser.parser.sqlnode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alibaba.rsqldb.parser.parser.SQLNodeParserFactory;
import com.alibaba.rsqldb.parser.parser.builder.AbstractSQLBuilder;
import com.alibaba.rsqldb.parser.parser.builder.UnionSQLBuilder;
import com.alibaba.rsqldb.parser.parser.result.BuilderParseResult;
import com.alibaba.rsqldb.parser.parser.result.IParseResult;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class UnionParser extends AbstractSqlNodeParser<SqlBasicCall, UnionSQLBuilder> {
    private static final Log LOG = LogFactory.getLog(UnionParser.class);

    @Override
    public IParseResult parse(UnionSQLBuilder builder, SqlBasicCall sqlBasicCall) {
        builder.setSqlNode(sqlBasicCall);
        List<SqlNode> sqlNodeList = sqlBasicCall.getOperandList();
        Set<String> tableNames = new HashSet<>();
        Map<String,List<AbstractSQLBuilder>> tableName2Builders=new HashMap<>();
        for (SqlNode sqlNode : sqlNodeList) {
            AbstractSQLBuilder sqlBuilder = SQLNodeParserFactory.parseBuilder(sqlNode);
            if (sqlBuilder instanceof UnionSQLBuilder) {
                UnionSQLBuilder unionSQLBuilder = (UnionSQLBuilder)sqlBuilder;
                for (AbstractSQLBuilder abstractSQLBuilder : unionSQLBuilder.getBuilders()) {
                    builder.addBuilder(abstractSQLBuilder);
                    tableNames.add(abstractSQLBuilder.getTableName());
                    add2Map(abstractSQLBuilder.getTableName(),abstractSQLBuilder,tableName2Builders);
                }
            } else {
                builder.addBuilder(sqlBuilder);
                add2Map(sqlBuilder.getTableName(),sqlBuilder,tableName2Builders);
                tableNames.add(sqlBuilder.getTableName());
            }

        }
        builder.setTableNames(tableNames);
        for(String tableName:tableNames){
            builder.addDependentTable(tableName);
        }
        if (tableNames.size() == 1) {
            builder.setTableName(tableNames.iterator().next());
        }else {
            String tableNameOfMaxSize=null;
            int size=0;
            for(String tableName:tableName2Builders.keySet()){
                if(tableNameOfMaxSize==null){
                    tableNameOfMaxSize=tableName;
                    size=tableName2Builders.get(tableName).size();
                }else {
                    if(size<tableName2Builders.get(tableName).size()){
                        tableNameOfMaxSize=tableName;
                        size=tableName2Builders.get(tableName).size();
                    }
                }
            }
            builder.setTableName(tableNameOfMaxSize);
        }

        return new BuilderParseResult(builder);
    }



    @Override
    public boolean support(Object sqlNode) {
        if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall)sqlNode;
            if (sqlBasicCall.getOperator().getName().toLowerCase().equals("union")) {
                return true;
            }
            if (sqlBasicCall.getOperator().getName().toLowerCase().equals("union all")) {
                return true;
            }
        }
        return false;
    }

    protected void add2Map(String name, AbstractSQLBuilder builder, Map<String, List<AbstractSQLBuilder>> builders) {
        List<AbstractSQLBuilder> list= builders.get(name);
        if(list==null){
            list=new ArrayList<>();
            builders.put(name,list);
        }
        list.add(builder);
    }

    @Override
    public UnionSQLBuilder create() {
        return new UnionSQLBuilder();
    }
}
