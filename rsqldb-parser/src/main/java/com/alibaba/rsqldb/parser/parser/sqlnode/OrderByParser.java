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

import com.alibaba.rsqldb.parser.parser.SqlNodeParserFactory;
import com.alibaba.rsqldb.parser.parser.builder.AbstractSqlBuilder;
import com.alibaba.rsqldb.parser.parser.builder.SelectSqlBuilder;
import com.alibaba.rsqldb.parser.parser.builder.SqlOrderByBuilder;
import com.alibaba.rsqldb.parser.parser.result.BuilderParseResult;
import com.alibaba.rsqldb.parser.parser.result.IParseResult;
import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;

public class OrderByParser extends AbstractSqlNodeNodeParser<SqlOrderBy, SqlOrderByBuilder> {

    @Override
    public IParseResult parse(SqlOrderByBuilder sqlOrderByBuilder, SqlOrderBy sqlOrderBy) {
        SqlNode sqlNode=sqlOrderBy.query;
        AbstractSqlBuilder builder = SqlNodeParserFactory.parseBuilder(sqlNode);
        ////保存所有create对应的builder， 在insert或维表join时使用
        builder.setTableName2Builders(sqlOrderByBuilder.getTableName2Builders());



        sqlOrderByBuilder.setSubSelect((SelectSqlBuilder) builder);

        sqlOrderByBuilder.setTableName(builder.getTableName());
        sqlOrderByBuilder.setSqlNode(sqlOrderBy);
        SqlNodeList orderList=sqlOrderBy.orderList;
        List<String> orderByFieldNames=new ArrayList<>();
        List<String> groupByFieldNames=new ArrayList<>();
        for(SqlNode subSQL:orderList.getList()){
            String orderElement="";
            if(SqlBasicCall.class.isInstance(subSQL)){
                SqlBasicCall sqlBasicCall=(SqlBasicCall)subSQL;
                if("DESC".equals(sqlBasicCall.getOperator().getName().toUpperCase())){
                    IParseResult result =parseSqlNode(sqlOrderByBuilder,sqlBasicCall.getOperandList().get(0));
                    orderElement=result.getReturnValue()+";false";
                    orderByFieldNames.add(orderElement);
                    groupByFieldNames.add(result.getReturnValue());
                }
            }else {
                IParseResult result =parseSqlNode(sqlOrderByBuilder,subSQL);
                orderElement=result.getReturnValue()+";true";
                orderByFieldNames.add(orderElement);
                groupByFieldNames.add(result.getReturnValue());
            }
        }
        sqlOrderByBuilder.setGroupByFieldNames(groupByFieldNames);
        sqlOrderByBuilder.setOrderFieldNames(orderByFieldNames);
        if(sqlOrderBy.fetch!=null){
            IParseResult fetchResult=parseSqlNode(sqlOrderByBuilder,sqlOrderBy.fetch);
            sqlOrderByBuilder.setFetchNextRows(Integer.valueOf(fetchResult.getReturnValue()));
        }

        if(sqlOrderBy.offset!=null){
            IParseResult offsetResult=parseSqlNode(sqlOrderByBuilder,sqlOrderBy.offset);
            sqlOrderByBuilder.setOffset(Integer.valueOf(offsetResult.getReturnValue()));
        }




        return new BuilderParseResult(sqlOrderByBuilder);
    }




    @Override
    public boolean support(Object sqlNode) {
        return sqlNode instanceof SqlOrderBy;
    }

    @Override
    public SqlOrderByBuilder create() {
        return new SqlOrderByBuilder();
    }
}
