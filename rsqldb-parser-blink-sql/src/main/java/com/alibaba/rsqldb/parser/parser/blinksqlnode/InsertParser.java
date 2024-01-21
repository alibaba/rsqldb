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
package com.alibaba.rsqldb.parser.parser.blinksqlnode;

import com.alibaba.rsqldb.parser.builder.InsertSqlBuilder;
import com.alibaba.rsqldb.parser.builder.WindowBuilder;
import com.alibaba.rsqldb.parser.sql.SQLParseContext;
import com.alibaba.rsqldb.parser.sqlnode.AbstractInsertParser;

import org.apache.calcite.sql.SqlEmit;
import org.apache.calcite.sql.SqlInsert;

public class InsertParser extends AbstractInsertParser {

    @Override
    protected void parseEmit(InsertSqlBuilder builder, SqlInsert insert) {
        SqlEmit sqlEmit = insert.getEmit();
        if (sqlEmit == null) {
            return;
        }
        if (sqlEmit.getBeforeDelay() != null) {
            WindowBuilder windowBuilder = (WindowBuilder)SQLParseContext.getWindowBuilder();
            if (windowBuilder != null) {
                long beforeValue = sqlEmit.getBeforeDelayValue();
                if (beforeValue > 0) {
                    windowBuilder.setEmitBefore(beforeValue / 1000);
                }
            }

        }
        if (sqlEmit.getAfterDelay() != null) {
            WindowBuilder windowBuilder = (WindowBuilder)SQLParseContext.getWindowBuilder();
            if (windowBuilder != null) {
                long afterValue = sqlEmit.getAfterDelayValue();
                if (afterValue > 0) {
                    windowBuilder.setEmitAfter(afterValue / 1000);
                }
            }

        }

    }

}
