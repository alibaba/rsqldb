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

import com.alibaba.rsqldb.parser.parser.SqlBuilderResult;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.streams.common.topology.ChainStage;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;

import org.apache.rocketmq.streams.script.utils.FunctionUtils;
import org.apache.rocketmq.streams.window.operator.impl.ShuffleOverWindow;

public class SqlOrderByBuilder extends SelectSqlBuilder {
    public static final String HITS_NAME="ORDER_BY";
    protected int offset;
    protected int fetchNextRows;

    protected List<String> orderFieldNames;//name contains 2 part:name;true/false

    protected List<String> groupByFieldNames;//name contains 2 part:name;true/false

    @Override
    public void build() {
        buildSql();
    }

    @Override public SqlBuilderResult buildSql() {
        SqlBuilderResult builderResult=null;
        if (subSelect != null) {
            PipelineBuilder subPipelineBuilder=createPipelineBuilder();
            subSelect.setPipelineBuilder(subPipelineBuilder);


            if (AbstractSqlBuilder.class.isInstance(subSelect)) {
                AbstractSqlBuilder abstractSQLBuilder = (AbstractSqlBuilder) subSelect;
                // abstractSQLBuilder.setTreeSQLBulider(getTreeSQLBulider());
                abstractSQLBuilder.setTableName2Builders(getTableName2Builders());
            }
            AbstractSqlBuilder abstractSQLBuilder = (AbstractSqlBuilder) subSelect;
            abstractSQLBuilder.addRootTableName(this.getRootTableNames());
            SqlBuilderResult sqlBuilderResult=subSelect.buildSql();
            mergeSQLBuilderResult(sqlBuilderResult);
            // sqlBuilderResult.getStageGroup().setSql(sqlFormatterUtil.format("CREATE VIEW "+createTable+" as \n"+PrintUtil.LINE+sqlBuilderResult.getStageGroup().getSql()));
            builderResult= sqlBuilderResult;
        }

        if(orderFieldNames==null){
            return builderResult;
        }
        int overWindowTopN=100;
        if(fetchNextRows>0){
            overWindowTopN=fetchNextRows+offset;
        }
        PipelineBuilder subPipelineBuilder=createPipelineBuilder();
        ShuffleOverWindow shuffleOverWindow = new ShuffleOverWindow();
        shuffleOverWindow.setTimeFieldName("");
       // shuffleOverWindow.setGroupByFieldName(MapKeyUtil.createKey(groupByFieldNames));
        shuffleOverWindow.setTopN(overWindowTopN);
        shuffleOverWindow.setOrderFieldNames(orderFieldNames);
        shuffleOverWindow.setStartRowNum(offset);
        setWindowInfoByHits(shuffleOverWindow,subSelect.getHits());
        subPipelineBuilder.addChainStage(shuffleOverWindow);
        SqlBuilderResult sqlBuilderResult=new SqlBuilderResult(subPipelineBuilder,this);
        mergeSQLBuilderResult(sqlBuilderResult);
        ChainStage<?> first= CollectionUtil.isNotEmpty(pipelineBuilder.getFirstStages())?pipelineBuilder.getFirstStages().get(0):null;
        SqlBuilderResult result= new SqlBuilderResult(pipelineBuilder,first,pipelineBuilder.getCurrentChainStage());
        String viewName=null;
        if(this.orderFieldNames!=null&&this.orderFieldNames.size()>0){
            viewName="Order By "+MapKeyUtil.createKey(this.orderFieldNames).replace(";true","").replace(";false"," DESC").replace(";",",");
        }else {

            viewName="limit "+(offset>0?offset+",":"")+fetchNextRows;
        }
        result.getStageGroup().setSql(viewName);
        result.getStageGroup().setViewName(viewName);
        return result;
    }

    protected void setWindowInfoByHits(ShuffleOverWindow window,Map<String, Map<String, String>> hits) {
        if(hits==null){
            return;
        }
        Map<String,String> hitsValue=hits.get(HITS_NAME);
        if(hitsValue==null){
            return;
        }

        if(hitsValue.containsKey(HITS_WINDOW_SIZE)){
            String value=hitsValue.get(HITS_WINDOW_SIZE);
            if(FunctionUtils.isLong(value)){
                Integer windowSize= Integer.valueOf(value);
                window.setSlideInterval(windowSize);
                window.setSizeInterval(windowSize);
            }

        }

        if(hitsValue.containsKey(HITS_WINDOW_BEFORE_EMIT)){
            String value=hitsValue.get(HITS_WINDOW_BEFORE_EMIT);
            if(FunctionUtils.isLong(value)){
                Long emitBefore= Long.valueOf(value);
                window.setEmitBeforeValue(emitBefore);
            }

        }
        if(hitsValue.containsKey(HITS_WINDOW_AFTER_EMIT)){
            String value=hitsValue.get(HITS_WINDOW_AFTER_EMIT);
            if(FunctionUtils.isLong(value)){
                Long emitAfter= Long.valueOf(value);
                window.setEmitAfterValue(emitAfter);
            }

        }
    }

    @Override
    public String getFieldName(String fieldName, boolean containsSelf) {
        if (subSelect != null && AbstractSqlBuilder.class.isInstance(subSelect)) {
            AbstractSqlBuilder abstractSQLBuilder = (AbstractSqlBuilder) subSelect;
            return abstractSQLBuilder.getFieldName(fieldName, containsSelf);
        }
        return null;
    }

    @Override
    public Set<String> getAllFieldNames() {
        return this.subSelect.getAllFieldNames();
    }

    @Override
    public Set<String> parseDependentTables() {
        if (subSelect != null) {
            if (AbstractSqlBuilder.class.isInstance(subSelect)) {
                AbstractSqlBuilder abstractSQLBuilder = (AbstractSqlBuilder) subSelect;
                // abstractSQLBuilder.setTreeSQLBulider(getTreeSQLBulider());
            }
            return subSelect.parseDependentTables();
        }
        return new HashSet<>();
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public int getFetchNextRows() {
        return fetchNextRows;
    }

    public void setFetchNextRows(int fetchNextRows) {
        this.fetchNextRows = fetchNextRows;
    }

    public List<String> getOrderFieldNames() {
        return orderFieldNames;
    }

    public void setOrderFieldNames(List<String> orderFieldNames) {
        this.orderFieldNames = orderFieldNames;
    }

    public List<String> getGroupByFieldNames() {
        return groupByFieldNames;
    }

    public void setGroupByFieldNames(List<String> groupByFieldNames) {
        this.groupByFieldNames = groupByFieldNames;
    }


}
