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
import com.alibaba.rsqldb.parser.parser.builder.CreateSqlBuilder;
import com.alibaba.rsqldb.parser.parser.builder.SelectSqlBuilder;
import com.alibaba.rsqldb.parser.parser.result.BuilderParseResult;
import com.alibaba.rsqldb.parser.parser.result.IParseResult;
import com.alibaba.rsqldb.parser.parser.sqlnode.AbstractSqlNodeNodeParser;
import com.alibaba.rsqldb.parser.util.SqlDataTypeUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.ddl.SqlTableOption;
import org.apache.rocketmq.streams.common.datatype.DataType;
import org.apache.rocketmq.streams.common.datatype.StringDataType;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.metadata.MetaDataField;
import org.apache.rocketmq.streams.common.utils.ContantsUtil;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

/**
 * Create Table Parser
 */
public class CreateParser extends AbstractSqlNodeNodeParser<SqlCreateTable, CreateSqlBuilder> {

    private static final Log LOG = LogFactory.getLog(CreateParser.class);

    @Override
    public IParseResult parse(CreateSqlBuilder createSQLBuilder, SqlCreateTable sqlCreateTable) {
        createSQLBuilder.setSqlNode(sqlCreateTable);
        createSQLBuilder.setSqlType("SOURCE");
        createSQLBuilder.setTableName(sqlCreateTable.getTableName().toString());
        createSQLBuilder.addCreatedTable(createSQLBuilder.getTableName());
        List<String> headerFieldNames=new ArrayList<>();
        MetaData metaData=createMetadata(createSQLBuilder,sqlCreateTable.getColumnList(),headerFieldNames);
        createSQLBuilder.setMetaData(metaData);
        createSQLBuilder.setHeaderFieldNames(headerFieldNames);
        createSQLBuilder.setProperties(createProperty(createSQLBuilder,sqlCreateTable));
        return new BuilderParseResult(createSQLBuilder);
    }

    @Override
    public boolean support(Object sqlNode) {
        if (sqlNode instanceof SqlCreateTable) {
            return true;
        }
        return false;
    }

    @Override
    public CreateSqlBuilder create() {
        return new CreateSqlBuilder();
    }


    /**
     * 把列转换成metadata
     *
     * @param sqlNodes
     * @return
     */
    public static MetaData createMetadata(CreateSqlBuilder builder,SqlNodeList sqlNodes, List<String> headerFildNames) {
        if (sqlNodes == null) {
            return null;
        }

        MetaData metaData = new MetaData();
        for (
            SqlNode sqlNode : sqlNodes) {
            if ((sqlNode instanceof SqlTableColumn.SqlMetadataColumn)) {
                SqlTableColumn.SqlMetadataColumn sqlMetadataColumn=(SqlTableColumn.SqlMetadataColumn)sqlNode;
                String fieldName = FunctionUtils.getConstant(sqlMetadataColumn.getName().toString());
                MetaDataField metaDataField = new MetaDataField();
                metaDataField.setDataType(new StringDataType());
                metaDataField.setFieldName(fieldName);
                metaData.getMetaDataFields().add(metaDataField);
                SelectSqlBuilder sqlBuilder=new SelectSqlBuilder();
                sqlBuilder.setCloseFieldCheck(true);
                sqlBuilder.setTableName(builder.getTableName());
                SqlNodeParserFactory.getParse(sqlNode).parse(sqlBuilder,sqlNode);
                builder.getScripts().addAll(sqlBuilder.getScripts());
                continue;
            }
            SqlTableColumn.SqlRegularColumn sqlTableColumn = ( SqlTableColumn.SqlRegularColumn)sqlNode;
            String fieldName = sqlTableColumn.getName().toString();
            String type = FunctionUtils.getConstant(sqlTableColumn.getType().toString());
            DataType dataType = SqlDataTypeUtil.covert(type);
            if (dataType == null) {
                LOG.warn("expect datatype, but convert fail.the sqlnode type is " + type);
            }
//            if(sqlTableColumn.isHeader()){
//                headerFildNames.add(fieldName);
//            }
            MetaDataField metaDataField = new MetaDataField();
            metaDataField.setDataType(dataType);
            metaDataField.setFieldName(fieldName);
            metaData.getMetaDataFields().add(metaDataField);
        }
        return metaData;
    }

    public Properties createProperty(CreateSqlBuilder createSQLBuilder,SqlCreateTable sqlCreateTable) {
        SqlNodeList sqlNodeList = sqlCreateTable.getPropertyList();
        List<SqlNode> propertys = sqlNodeList.getList();
        /**
         * 把property的值，生成key：value。和key：propertysql的值
         */
        Properties properties = new Properties();
        List<SqlNode> sqlNodes=sqlNodeList.getList();
        String type = "sls";
        int i = 0;
        for (SqlNode sqlNode : propertys) {
            if (!SqlTableOption.class.isInstance(sqlNode)) {
                i++;
                continue;
            }
            SqlTableOption property = (SqlTableOption)sqlNode;
            String value = property.getValueString();
            if (ContantsUtil.isContant(value)) {
                value = value.substring(1, value.length() - 1);
            }
            String key=property.getKeyString().toLowerCase();
            if (ContantsUtil.isContant(key)) {
                key = key.substring(1, key.length() - 1);
            }
            properties.put(key, value);
            createSQLBuilder.getKey2PropertyItem().put(property.getKeyString(), property.toString());
            createSQLBuilder.getKey2PropertyItem().put(key, property.toString());
            i++;
        }
        return properties;
    }
}
