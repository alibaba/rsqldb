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
package com.alibaba.rsqldb.parser.util;

import com.alibaba.rsqldb.parser.parser.SQLNodeParserFactory;
import com.alibaba.rsqldb.parser.parser.builder.CreateSQLBuilder;
import com.alibaba.rsqldb.parser.parser.builder.SelectSQLBuilder;
import java.util.List;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.rocketmq.streams.common.datatype.DataType;
import org.apache.rocketmq.streams.common.datatype.StringDataType;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.metadata.MetaDataField;

public class ColumnUtil {
    private static final Log LOG = LogFactory.getLog(ColumnUtil.class);

    /**
     * 把列转换成metadata
     *
     * @param sqlNodes
     * @return
     */
    public static MetaData createMetadata(CreateSQLBuilder builder, SqlNodeList sqlNodes,
        List<String> headerFildNames) {
        if (sqlNodes == null) {
            return null;
        }

        MetaData metaData = new MetaData();
        for (
            SqlNode sqlNode : sqlNodes) {
            if (!(sqlNode instanceof SqlTableColumn)) {
                if (SqlBasicCall.class.isInstance(sqlNode)) {
                    SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
                    if (sqlBasicCall.getOperator().getName().toLowerCase().equals("as")) {
                        String fieldName = sqlBasicCall.getOperandList().get(1).toString();
                        MetaDataField metaDataField = new MetaDataField();
                        metaDataField.setDataType(new StringDataType());
                        metaDataField.setFieldName(fieldName);
                        metaData.getMetaDataFields().add(metaDataField);
                        SelectSQLBuilder sqlBuilder = new SelectSQLBuilder();
                        sqlBuilder.setCloseFieldCheck(true);
                        sqlBuilder.setTableName(builder.getTableName());
                        SQLNodeParserFactory.getParse(sqlNode).parse(sqlBuilder, sqlNode);
                        builder.getScripts().addAll(sqlBuilder.getScripts());
                        continue;
                    }
                }
            }
            SqlTableColumn sqlTableColumn = (SqlTableColumn) sqlNode;
            String fieldName = sqlTableColumn.getName().toString();
            String type = sqlTableColumn.getType().toString();
            DataType dataType = SqlDataTypeUtil.covert(type);
            if (dataType == null) {
                LOG.warn("expect datatype, but convert fail.the sqlnode type is " + type);
            }
            if (sqlTableColumn.isHeader()) {
                headerFildNames.add(fieldName);
            }
            MetaDataField metaDataField = new MetaDataField();
            metaDataField.setDataType(dataType);
            metaDataField.setFieldName(fieldName);
            metaData.getMetaDataFields().add(metaDataField);
        }
        return metaData;
    }

}
