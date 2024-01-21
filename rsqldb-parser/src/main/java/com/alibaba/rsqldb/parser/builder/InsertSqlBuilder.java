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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.alibaba.rsqldb.parser.SqlBuilderResult;
import com.alibaba.rsqldb.parser.sql.stage.MergeSQLJobStage;

import org.apache.rocketmq.streams.common.channel.impl.view.ViewSink;
import org.apache.rocketmq.streams.common.channel.sink.ISink;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.common.topology.stages.OutputChainStage;
import org.apache.rocketmq.streams.common.utils.ContantsUtil;
import org.apache.rocketmq.streams.common.utils.PrintUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.operator.impl.ScriptOperator;

public class InsertSqlBuilder extends AbstractSqlBuilder {

    protected AbstractSqlBuilder builder;
    /**
     * 创建表的descriptor，在生成build树时，赋值
     */
    protected CreateSqlBuilder createBuilder;

    //protected MetaData metaData;
    private List<String> columnNames;

    @Override
    public void build() {
        buildSql();
    }

    @Override
    public SqlBuilderResult buildSql() {
        SqlBuilderResult sqlBuilderResult = null;
        ISink<?> sink = null;
        if (createBuilder != null) {
            createBuilder.setPipelineBuilder(getPipelineBuilder());
            sink = createBuilder.createSink();
        }

        if (builder != null) {
            if (builder instanceof SelectSqlBuilder && sink instanceof ViewSink) {
                SelectSqlBuilder sqlBuilder = (SelectSqlBuilder)builder;
                sqlBuilder.setViewSink(true);
            }
            builder.setConfiguration(getConfiguration());
            PipelineBuilder pipelineBuilder = createPipelineBuilder();
            builder.setPipelineBuilder(pipelineBuilder);
            sqlBuilderResult = builder.buildSql();
            mergeSQLBuilderResult(sqlBuilderResult);
        }
        OutputChainStage<?> chainStage = null;
        //如果是规则引擎，需要输出出去，则在这里生成channel
        if (createBuilder != null) {
            //让自动生成名字，否则会和数据源的名字冲突
            sink.setName(null);
            if (columnNames != null && columnNames.size() > 0 && builder instanceof SelectSqlBuilder) {
                StringBuilder script = new StringBuilder();
                SelectSqlBuilder sqlBuilder = (SelectSqlBuilder)builder;
                int i = 0;
                for (String column : columnNames) {
                    script.append(column).append("=").append(sqlBuilder.getFieldNamesOrderByDeclare().get(i)).append(";");
                    i++;
                }
                pipelineBuilder.addChainStage(new ScriptOperator(script.toString()));
            }

            if (sink instanceof ViewSink) {
                MergeSQLJobStage mergeSQLJobStage = new MergeSQLJobStage();
                sink.addConfigurables(pipelineBuilder);
                mergeSQLJobStage.setSink(sink);
                pipelineBuilder.addChainStage(mergeSQLJobStage);
                chainStage = mergeSQLJobStage;
            } else {
                chainStage = pipelineBuilder.addOutput(sink);
            }

            pipelineBuilder.setHorizontalStages(chainStage);
            pipelineBuilder.setCurrentChainStage(chainStage);

            if (chainStage == null) {
                return sqlBuilderResult;
            }
            String type = createBuilder.getProperties().getProperty("type");
            if (StringUtil.isEmpty(type)) {
                type = createBuilder.getProperties().getProperty("TYPE");
            }
            if (StringUtil.isEmpty(type)) {
                type = createBuilder.getProperties().getProperty("connector");
            }
            if (StringUtil.isEmpty(type)) {
                type = createBuilder.getProperties().getProperty("CONNECTOR");
            }
            if (ContantsUtil.isContant(type)) {
                type = type.substring(1, type.length() - 1);
            }

            chainStage.setEntityName(type);
        }
        pipelineBuilder.setCurrentStageGroup(null);
        SqlBuilderResult result = new SqlBuilderResult(pipelineBuilder, sqlBuilderResult.getFirstStage(), pipelineBuilder.getCurrentChainStage());
        String sql = sqlBuilderResult.getStageGroup().getViewName();
        if (!sql.toLowerCase().startsWith("from")) {
            sql = "FROM " + sql;
        }
        result.getStageGroup().setSql("INSERT INOT " + getTableName() + " " + sql);
        result.getStageGroup().setViewName("INSERT INOT " + getTableName());
        chainStage.setDiscription("Insert");
        chainStage.setSql("INSERT INOT " + getTableName() + " " + sql);
        return result;
    }

    @Override
    public String getFieldName(String fieldName, boolean containsSelf) {
        return null;
    }

    @Override
    public String createSql() {
        String sql = null;
        if (createBuilder != null) {
            sql = createBuilder.createSql() + ";" + PrintUtil.LINE;
        }
        sql += super.createSql();
        return sql;
    }

    public AbstractSqlBuilder getSqlDescriptor() {
        return builder;
    }

    public void setSqlDescriptor(AbstractSqlBuilder sqlDescriptor) {
        this.builder = sqlDescriptor;
    }

    @Override
    public Set<String> parseDependentTables() {
        Set<String> dependentTables = new HashSet<>();
        if (builder != null) {
            return builder.parseDependentTables();
        }
        return dependentTables;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(List<String> columnNames) {
        this.columnNames = columnNames;
    }

    public CreateSqlBuilder getCreateBuilder() {
        return createBuilder;
    }

    public void setCreateBuilder(CreateSqlBuilder createBuilder) {
        this.createBuilder = createBuilder;
    }

}
