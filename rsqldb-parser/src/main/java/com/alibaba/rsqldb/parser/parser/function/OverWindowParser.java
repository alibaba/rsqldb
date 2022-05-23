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
package com.alibaba.rsqldb.parser.parser.function;

import com.alibaba.rsqldb.parser.parser.builder.SelectSQLBuilder;
import com.alibaba.rsqldb.parser.parser.builder.WindowBuilder;
import com.alibaba.rsqldb.parser.parser.result.IParseResult;
import com.alibaba.rsqldb.parser.parser.result.VarParseResult;
import com.alibaba.rsqldb.parser.parser.sqlnode.AbstractSelectNodeParser;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWindow;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.model.NameCreatorContext;

public class OverWindowParser extends AbstractSelectNodeParser<SqlBasicCall> {

    private static final Log LOG = LogFactory.getLog(SelectSQLBuilder.class);

    @Override
    public IParseResult parse(SelectSQLBuilder builder, SqlBasicCall sqlBasicCall) {
        SqlWindow sqlWindow = (SqlWindow) sqlBasicCall.getOperandList().get(1);
        SqlNodeList sqlNodeList = sqlWindow.getPartitionList();
        List<String> partionFieldNames = new ArrayList<>();
        for (int i = 0; i < sqlNodeList.size(); i++) {
            IParseResult result = parseSqlNode(builder, sqlNodeList.get(i));
            partionFieldNames.add(result.getReturnValue());
        }
        String rowNumberName = NameCreatorContext.get().createNewName("over", "parition");
        WindowBuilder windowBuilder = new WindowBuilder();
        windowBuilder.setGroupByFieldNames(partionFieldNames);
        windowBuilder.setOverWindowName(rowNumberName);
        builder.setWindowBuilder(windowBuilder);
        builder.setOverName(rowNumberName);
        VarParseResult varParseResult = new VarParseResult(rowNumberName);
        boolean isShuffleOver = true;
        if ((sqlWindow.getOrderList() == null || sqlWindow.getOrderList().size() == 0) || (sqlWindow.getOrderList().size() == 1 && "`proctime`()".equals(sqlWindow.getOrderList().get(0).toString().toLowerCase()))) {
            isShuffleOver = false;
        } else {
            List<String> shuffleOverWindowOrderByFieldNames = new ArrayList<>();
            SqlNodeList orderNodes = sqlWindow.getOrderList();
            for (SqlNode sqlNode : orderNodes.getList()) {
                String orderByStr = createOrderByStr(sqlNode);
                shuffleOverWindowOrderByFieldNames.add(orderByStr);
            }
            windowBuilder.setShuffleOverWindowOrderByFieldNames(shuffleOverWindowOrderByFieldNames);
        }
        windowBuilder.setShuffleOverWindow(isShuffleOver);
        return varParseResult;
    }

    @Override
    public boolean support(Object sqlNode) {
        if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
            if (sqlBasicCall.getOperator().getName().toLowerCase().equals("over")) {
                return true;
            }
        }
        return false;
    }

    private String createOrderByStr(SqlNode node) {
        if (SqlIdentifier.class.isInstance(node)) {
            return node.toString() + ";true";
        } else {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) node;
            return sqlBasicCall.getOperandList().get(0) + ";false";
        }
    }
}
