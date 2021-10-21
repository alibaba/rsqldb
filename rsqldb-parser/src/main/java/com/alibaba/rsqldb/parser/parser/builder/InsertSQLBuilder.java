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
package com.alibaba.rsqldb.parser.parser.builder;

import java.util.HashSet;
import java.util.Set;

import org.apache.rocketmq.streams.common.channel.sink.ISink;
import org.apache.rocketmq.streams.common.topology.stages.OutputChainStage;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.utils.ContantsUtil;
import org.apache.rocketmq.streams.common.utils.PrintUtil;
import com.alibaba.rsqldb.parser.util.ColumnUtil;
import org.apache.calcite.sql.SqlNodeList;

public class InsertSQLBuilder extends AbstractSQLBuilder {

    protected AbstractSQLBuilder builder;

    protected MetaData metaData;

    /**
     * 创建表的descriptor，在生成build树时，赋值
     */
    protected CreateSQLBuilder createBuilder;

    @Override
    public void build() {
        if (builder != null) {
            //   builder.setTreeSQLBulider(getTreeSQLBulider());
            builder.setPipelineBuilder(pipelineBuilder);
            builder.buildSQL();
        }
        /**
         * 如果是规则引擎，需要输出出去，则在这里生成channel
         */
        if (createBuilder != null) {
            createBuilder.setPipelineBuilder(getPipelineBuilder());
            ISink channel = createBuilder.createSink();
            channel.setConfigureName(null);//让自动生成名字，否则会和数据源的名字冲突
            if (channel != null) {
                OutputChainStage chainStage = pipelineBuilder.addOutput(channel);
                if (chainStage == null) {
                    return;
                }
                String type = createBuilder.createProperty().getProperty("type");
                if (ContantsUtil.isContant(type)) {
                    type = type.substring(1, type.length() - 1);
                }
                chainStage.setEntityName(type);
            }
        }
    }

    @Override
    public String getFieldName(String fieldName, boolean containsSelf) {
        return null;
    }

    @Override
    public String createSQL() {
        String sql = null;
        if (createBuilder != null) {
            sql = createBuilder.createSQL() + ";" + PrintUtil.LINE;
        }
        sql += super.createSQL();
        return sql;
    }

    public AbstractSQLBuilder getSqlDescriptor() {
        return builder;
    }

    public void setSqlDescriptor(AbstractSQLBuilder sqlDescriptor) {
        this.builder = sqlDescriptor;
    }

    @Override
    public Set<String> parseDependentTables() {
        Set<String> dependentTables = new HashSet<>();
        //dependentTables.add(getTableName());
        if (builder != null) {
            // builder.setTreeSQLBulider(getTreeSQLBulider());
            return builder.parseDependentTables();
        }
        return dependentTables;
    }

    /**
     * 把创建表的语句转换成metadata
     *
     * @param sqlNodes
     */
    public void createColumn(SqlNodeList sqlNodes) {
        MetaData metaData = ColumnUtil.createMetadata(createBuilder,sqlNodes);
        this.metaData = metaData;
    }

    public CreateSQLBuilder getCreateBuilder() {
        return createBuilder;
    }

    public void setCreateBuilder(CreateSQLBuilder createBuilder) {
        this.createBuilder = createBuilder;
    }

    public MetaData getMetaData() {
        return metaData;
    }

    public void setMetaData(MetaData metaData) {
        this.metaData = metaData;
    }
}
