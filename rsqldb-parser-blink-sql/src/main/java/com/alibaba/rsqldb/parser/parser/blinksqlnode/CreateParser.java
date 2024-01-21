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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.alibaba.rsqldb.parser.SqlNodeParserFactory;
import com.alibaba.rsqldb.parser.builder.CreateSqlBuilder;
import com.alibaba.rsqldb.parser.builder.SelectSqlBuilder;
import com.alibaba.rsqldb.parser.result.BuilderParseResult;
import com.alibaba.rsqldb.parser.result.IParseResult;
import com.alibaba.rsqldb.parser.sqlnode.AbstractSqlNodeNodeParser;
import com.alibaba.rsqldb.parser.util.SqlDataTypeUtil;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlProperty;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.ddl.SqlWatermark;
import org.apache.rocketmq.streams.common.datatype.DataType;
import org.apache.rocketmq.streams.common.datatype.StringDataType;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.metadata.MetaDataField;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.common.utils.ContantsUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;

/**
 * Create Table Parser
 */
public class CreateParser extends AbstractSqlNodeNodeParser<SqlCreateTable, CreateSqlBuilder> {

    private static final Log LOG = LogFactory.getLog(CreateParser.class);

    /**
     * 把列转换成metadata
     *
     * @param sqlNodes
     * @return
     */
    public static MetaData createMetadata(CreateSqlBuilder builder, SqlNodeList sqlNodes, List<String> headerFieldNames) {
        if (sqlNodes == null) {
            return null;
        }

        MetaData metaData = new MetaData();
        for (
            SqlNode sqlNode : sqlNodes) {
            if (!(sqlNode instanceof SqlTableColumn)) {
                if (sqlNode instanceof SqlBasicCall) {
                    SqlBasicCall sqlBasicCall = (SqlBasicCall)sqlNode;
                    if (sqlBasicCall.getOperator().getName().equalsIgnoreCase("as")) {
                        String fieldName = sqlBasicCall.getOperandList().get(1).toString();
                        MetaDataField metaDataField = new MetaDataField();
                        metaDataField.setDataType(new StringDataType());
                        metaDataField.setFieldName(fieldName);
                        metaData.getMetaDataFields().add(metaDataField);
                        SelectSqlBuilder sqlBuilder = new SelectSqlBuilder();
                        sqlBuilder.setCloseFieldCheck(true);
                        sqlBuilder.setTableName(builder.getTableName());
                        sqlBuilder.setConfiguration(builder.getConfiguration());
                        SqlNodeParserFactory.getParse(sqlNode).parse(sqlBuilder, sqlNode);
                        builder.getScripts().addAll(sqlBuilder.getScripts());
                        continue;
                    }
                }
            }
            SqlTableColumn sqlTableColumn = (SqlTableColumn)sqlNode;
            String fieldName = sqlTableColumn.getName().toString();
            String type = sqlTableColumn.getType().toString();
            DataType dataType = SqlDataTypeUtil.covert(type);
            if (dataType == null) {
                LOG.warn("expect datatype, but convert fail.the sqlnode type is " + type);
            }
            if (sqlTableColumn.isHeader()) {
                headerFieldNames.add(fieldName);
            }
            MetaDataField metaDataField = new MetaDataField();
            metaDataField.setDataType(dataType);
            metaDataField.setFieldName(fieldName);
            metaData.getMetaDataFields().add(metaDataField);
        }
        return metaData;
    }

    @Override
    public IParseResult parse(CreateSqlBuilder createSQLBuilder, SqlCreateTable sqlCreateTable) {
        createSQLBuilder.setSqlNode(sqlCreateTable);
        createSQLBuilder.setSqlType(sqlCreateTable.getTableType());
        createSQLBuilder.setTableName(sqlCreateTable.getTableName().toString());
        createSQLBuilder.addCreatedTable(createSQLBuilder.getTableName());

        createSQLBuilder.setProperties(createProperty(createSQLBuilder, sqlCreateTable));

        /**
         * parse water mark
         */
        SqlWatermark sqlWatermark = sqlCreateTable.getWatermark();
        if (sqlWatermark != null) {
            CreateSqlBuilder.WaterMark waterMark = new CreateSqlBuilder.WaterMark();
            if (sqlWatermark.getColumnName() != null) {
                String fieldName = parseSqlNode(createSQLBuilder, sqlWatermark.getColumnName()).getReturnValue();
                waterMark.setTimeFieldName(fieldName);
            }
            if (sqlWatermark.getFunctionCall() != null) {
                List<SqlNode> sqlNodes = sqlWatermark.getFunctionCall().getOperandList();
                SqlNode waterMarkSqlNode = null;
                if (sqlNodes.size() == 1) {
                    waterMarkSqlNode = sqlNodes.get(0);
                } else {
                    waterMarkSqlNode = sqlNodes.get(sqlNodes.size() - 1);
                }
                String value = parseSqlNode(createSQLBuilder, waterMarkSqlNode).getReturnValue();
                if (StringUtil.isNotEmpty(value)) {
                    waterMark.setWaterMarkSecond(Integer.parseInt(value) / 1000);
                }
            }
            createSQLBuilder.setWaterMark(waterMark);
        }

        /**
         * parse index
         */
        SqlNodeList primary = sqlCreateTable.getPrimaryKeyList();
        List<String> primaryFieldNames = null;
        if (primary != null) {
            primaryFieldNames = new ArrayList<>();
            for (SqlNode sqlNode : primary.getList()) {
                String fieldName = parseSqlNode(createSQLBuilder, sqlNode).getReturnValue();
                primaryFieldNames.add(fieldName);
            }
        }
        List<String> uniqueIndexFieldNames = null;
        List<List<String>> indexFieldNamesList = null;
        List<SqlCreateTable.IndexWrapper> indexs = sqlCreateTable.getIndexKeysList();
        if (indexs != null) {
            uniqueIndexFieldNames = new ArrayList<>();
            indexFieldNamesList = new ArrayList<>();
            for (SqlCreateTable.IndexWrapper indexWrapper : indexs) {
                boolean isUnique = indexWrapper.unique;
                List<String> indexFieldNames = null;
                if (!isUnique) {
                    indexFieldNames = new ArrayList<>();
                    indexFieldNamesList.add(indexFieldNames);
                }
                for (SqlNode sqlNode : indexWrapper.indexKeys.getList()) {
                    String fieldName = parseSqlNode(createSQLBuilder, sqlNode).getReturnValue();
                    if (isUnique) {
                        uniqueIndexFieldNames.add(fieldName);
                    } else {
                        indexFieldNames.add(fieldName);
                    }
                }

            }
        }
        createColumn(createSQLBuilder, sqlCreateTable.getColumnList(), primaryFieldNames, uniqueIndexFieldNames, indexFieldNamesList);

        return new BuilderParseResult(createSQLBuilder);
    }

    /**
     * 把创建表的语句转换成metadata
     *
     * @param sqlNodes
     * @param primaryFieldNames
     * @param uniqueIndexFieldNames
     * @param indexFieldNamesList
     */
    public void createColumn(CreateSqlBuilder createSQLBuilder, SqlNodeList sqlNodes, List<String> primaryFieldNames,
        List<String> uniqueIndexFieldNames, List<List<String>> indexFieldNamesList) {
        List<String> headerFieldNames = new ArrayList<>();
        MetaData metaData = createMetadata(createSQLBuilder, sqlNodes, headerFieldNames);
        createSQLBuilder.setMetaData(metaData);
        createSQLBuilder.setHeaderFieldNames(headerFieldNames);
        if (CollectionUtil.isNotEmpty(primaryFieldNames)) {
            metaData.setPrimaryFieldNames(MapKeyUtil.createKey(",", primaryFieldNames));
        }
        if (CollectionUtil.isNotEmpty(uniqueIndexFieldNames)) {
            metaData.setUniqueIndexFieldNames(MapKeyUtil.createKey(",", uniqueIndexFieldNames));
        }
        if (CollectionUtil.isNotEmpty(indexFieldNamesList)) {
            List<String> indexs = new ArrayList<>();
            for (List<String> subIndexs : indexFieldNamesList) {
                if (CollectionUtil.isNotEmpty(subIndexs)) {
                    String indexNames = MapKeyUtil.createKey(",", subIndexs);
                    indexs.add(indexNames);
                }

            }
            if (indexs.size() > 0) {
                metaData.setIndexFieldNamesList(indexs);
            }
        }
    }

    public Properties createProperty(CreateSqlBuilder createSQLBuilder, SqlCreateTable sqlCreateTable) {
        SqlNodeList sqlNodeList = sqlCreateTable.getPropertyList();
        List<SqlNode> propertys = sqlNodeList.getList();
        /**
         * 把property的值，生成key：value。和key：propertysql的值
         */
        //把property的值，生成key：value。和key：propertysql的值
        Properties properties = new Properties();
        SqlProperty sqlProperty = null;
        String type = "sls";
        int i = 0;
        for (SqlNode sqlNode : propertys) {
            if (!SqlProperty.class.isInstance(sqlNode)) {
                i++;
                continue;
            }
            SqlProperty property = (SqlProperty)sqlNode;
            String value = property.getValueString();
            if (ContantsUtil.isContant(value)) {
                value = value.substring(1, value.length() - 1);
            }
            String key = property.getKeyString().toLowerCase();
            properties.put(property.getKeyString(), value);
            properties.put(key, value);
            if ("type".equals(key)) {
                type = property.getValueString();
            }

            createSQLBuilder.getKey2PropertyItem().put(property.getKeyString(), property.toString());
            createSQLBuilder.getKey2PropertyItem().put(key, property.toString());
            i++;
        }
        return properties;
    }

    @Override
    public boolean support(Object sqlNode) {
        if (sqlNode instanceof SqlCreateTable) {
            return true;
        }
        return false;
    }

    @Override
    public CreateSqlBuilder create(Properties configuration) {
        CreateSqlBuilder createSqlBuilder = new CreateSqlBuilder();
        createSqlBuilder.setConfiguration(configuration);
        return createSqlBuilder;
    }
}
