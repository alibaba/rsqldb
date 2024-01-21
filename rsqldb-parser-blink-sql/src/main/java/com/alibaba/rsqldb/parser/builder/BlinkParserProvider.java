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

import java.util.ArrayList;
import java.util.List;

import com.alibaba.rsqldb.parser.sql.AbstractSqlParser;
import com.alibaba.rsqldb.parser.sql.IParserProvider;
import com.alibaba.rsqldb.parser.sql.ISqlParser;

import com.google.auto.service.AutoService;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.sql.parser.ddl.SqlNodeInfo;
import org.apache.flink.sql.parser.util.SqlContextUtils;
import org.apache.rocketmq.streams.common.model.ServiceName;

@AutoService(IParserProvider.class)
@ServiceName("blink")
public class BlinkParserProvider implements IParserProvider {

    @Override
    public ISqlParser createSqlParser() {
        return new AbstractSqlParser() {

            @Override
            protected List<SqlNode> parseSql(String sql) {
                try {
                    List<SqlNodeInfo> sqlNodeInfoList = SqlContextUtils.parseContext(sql);
                    List<SqlNode> sqlNodeList = new ArrayList<>();
                    for (SqlNodeInfo sqlNodeInfo : sqlNodeInfoList) {
                        sqlNodeList.add(sqlNodeInfo.getSqlNode());
                    }
                    return sqlNodeList;
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }
        };
    }

    @Override
    public String getName() {
        return "blink";
    }
}
