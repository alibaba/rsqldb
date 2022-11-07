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

import com.alibaba.rsqldb.parser.parser.builder.SelectSqlBuilder;
import com.alibaba.rsqldb.parser.parser.function.HopParser;
import com.alibaba.rsqldb.parser.parser.function.TumbleParser;
import com.alibaba.rsqldb.parser.parser.sqlnode.AbstractSelectParser;
import java.util.List;
import java.util.Map;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlHint;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlTableRef;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

public class FlinkSelectParser extends AbstractSelectParser {
    @Override protected boolean parseFrom(SelectSqlBuilder selectSQLBuilder, SqlNode from,String aliasName ) {
        SqlNode realFrom=from;
        String realAliasName=aliasName;

        if (from instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) from;
            if (sqlBasicCall.getOperator().getName().toLowerCase().equals("as")) {
                from = sqlBasicCall.getOperandList().get(0);
                realAliasName = sqlBasicCall.getOperandList().get(1).toString();
            }
        }


        if (from instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) from;
            if (sqlBasicCall.getOperator().getName().toLowerCase().equals("table")) {
                from = parseWindow(selectSQLBuilder,sqlBasicCall.getOperandList().get(0));
                realFrom=from;
            }
        }

        if(SqlTableRef.class.isInstance(from)){
            SqlTableRef sqlTableRef=(SqlTableRef)from;
            List<SqlNode> sqlNodeList=sqlTableRef.getOperandList();
            if(sqlNodeList.size()==2){
                realFrom=sqlTableRef.getOperandList().get(0);
                SqlHint hits = (SqlHint) ((SqlNodeList) sqlNodeList.get(1)).get(0);
                Map<String,String> parameters=  hits.getOptionKVPairs();
                selectSQLBuilder.getHits().put(hits.getName(),parameters);
            }
        }
        return super.parseFrom(selectSQLBuilder, realFrom,realAliasName);
    }

    private SqlNode parseWindow(SelectSqlBuilder builder, SqlNode windowNode) {
        if(SqlBasicCall.class.isInstance(windowNode)){
            SqlNode from=windowNode;
            String fieldName=null;
            SqlIntervalLiteral windowSize=null;
            SqlIntervalLiteral step=null;
            SqlBasicCall sqlBasicCall=(SqlBasicCall)windowNode;
            String windowType= FunctionUtils.getConstant(sqlBasicCall.getOperator().toString());
            List<SqlNode> sqlNodeList=sqlBasicCall.getOperandList();
            int index=0;
            if(sqlNodeList.size()>index){
                SqlNode tableNode=sqlNodeList.get(index);
                if(SqlBasicCall.class.isInstance(tableNode)){
                    SqlBasicCall tableSqlBasicCall=(SqlBasicCall)tableNode;
                    if("TABLE".equals(tableSqlBasicCall.getOperator().getName().toUpperCase())){
                        from= tableSqlBasicCall.getOperandList().get(index);
                        index++;
                    }

                }
            }
            if(sqlNodeList.size()>index){
                SqlNode tableNode=sqlNodeList.get(index);
                if(SqlBasicCall.class.isInstance(tableNode)){
                    SqlBasicCall tableSqlBasicCall=(SqlBasicCall)tableNode;
                    if("DESCRIPTOR".equals(tableSqlBasicCall.getOperator().getName().toUpperCase())){
                        SqlNode fieldNode= tableSqlBasicCall.getOperandList().get(index);
                        fieldName=fieldNode.toString();
                        index++;
                    }

                }
            }

            if(sqlNodeList.size()>index){

                SqlNode tableNode=sqlNodeList.get(index);
                if(SqlIntervalLiteral.class.isInstance(tableNode)){
                    windowSize=(SqlIntervalLiteral)tableNode;
                    index++;
                }
            }
            if(sqlNodeList.size()>index){

                SqlNode tableNode=sqlNodeList.get(index);
                if(SqlIntervalLiteral.class.isInstance(tableNode)){
                    step=(SqlIntervalLiteral)tableNode;
                    index++;
                }
            }
            if("CUMULATE".equals(windowType.toUpperCase())){
                TumbleParser.createWindowBuilder(builder,step,windowSize,fieldName);
            }
           else if("TUMBLE".equals(windowType.toUpperCase())){
                TumbleParser.createWindowBuilder(builder,windowSize,fieldName);
            }else if("HOP".equals(windowType.toUpperCase())){
                if(step==null){
                    step=windowSize;
                }
                HopParser.createWindowBuilder(builder,step,windowSize,fieldName);
            }else  {
                throw new RuntimeException("can not support the window type "+windowType);
            }

            return from;
        }
        return windowNode;
    }
}
