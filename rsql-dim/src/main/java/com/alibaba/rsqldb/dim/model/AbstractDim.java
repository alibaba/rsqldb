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
package com.alibaba.rsqldb.dim.model;

import com.alibaba.fastjson.JSONObject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.cache.ByteArrayMemoryTable;
import org.apache.rocketmq.streams.common.cache.MappedByteBufferTable;
import org.apache.rocketmq.streams.common.cache.compress.AbstractMemoryTable;
import org.apache.rocketmq.streams.common.cache.softreference.ICache;
import org.apache.rocketmq.streams.common.cache.softreference.impl.SoftReferenceCache;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.configure.ConfigureFileKey;
import org.apache.rocketmq.streams.common.utils.DataTypeUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import com.alibaba.rsqldb.dim.index.DimIndex;
import com.alibaba.rsqldb.dim.index.IndexExecutor;
import org.apache.rocketmq.streams.filter.builder.ExpressionBuilder;
import org.apache.rocketmq.streams.filter.function.expression.Equals;
import org.apache.rocketmq.streams.filter.operator.Rule;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;
import org.apache.rocketmq.streams.filter.operator.expression.RelationExpression;
import org.apache.rocketmq.streams.script.ScriptComponent;

/**
 * ??????????????????????????? ?????????????????????????????????
 */
public abstract class AbstractDim extends BasedConfigurable {

    private static final Log LOG = LogFactory.getLog(AbstractDim.class);

    public static final String TYPE = "nameList";

    /**
     * ?????????????????????????????????????????????
     */
    protected Long pollingTimeMinute = 60L;

    /**
     * ??????????????????????????????????????????????????????????????????????????????string??????;?????? ?????????????????????????????????????????????????????????Map<String,List<RowId>?????????????????????????????????????????????????????????????????? ?????????????????????1.name 2. ip;address
     */
    protected List<String> indexs = new ArrayList<>();

    protected boolean isLarge = false;//if isLarge=true use MapperByteBufferTable ????????????

    protected String filePath;
    /**
     * ???????????????????????????????????????CompressTable???
     */
    protected transient volatile AbstractMemoryTable dataCache;

    /**
     * ?????????????????????????????????????????????????????????????????????key???row???datacache???index??????value????????????????????????????????????row key??????????????? value???row???dataCache???index??????value????????????????????????????????????row
     */
    protected transient DimIndex nameListIndex;
    protected transient Set<String> columnNames;
    //??????????????????????????????
    protected transient ScheduledExecutorService executorService;

    public AbstractDim() {
        this.setType(TYPE);
    }

    //protected String index;//?????????????????????????????????indexs?????????

    public String addIndex(String... fieldNames) {
        return addIndex(this.indexs, fieldNames);
    }

    @Override
    protected boolean initConfigurable() {
        boolean success = super.initConfigurable();
        if (Boolean.TRUE.equals(Boolean.valueOf(ComponentCreator.getProperties().getProperty(ConfigureFileKey.DIPPER_RUNNING_STATUS, ConfigureFileKey.DIPPER_RUNNING_STATUS_DEFAULT)))) {
            loadNameList();
            executorService = new ScheduledThreadPoolExecutor(3);
            executorService.scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    loadNameList();
                }
            }, pollingTimeMinute, pollingTimeMinute, TimeUnit.MINUTES);
        }

        return success;
    }

    /**
     * ?????????????????? ????????????
     */
    protected void loadNameList() {
        try {
            LOG.info(getConfigureName() + " begin polling data");
            //????????????
            AbstractMemoryTable dataCacheVar = loadData();
            this.dataCache = dataCacheVar;
            this.nameListIndex = buildIndex(dataCacheVar);
            this.columnNames = this.dataCache.getCloumnName2Index().keySet();
        } catch (Exception e) {
            LOG.error("Load configurables error:" + e.getMessage(), e);
        }
    }

    /**
     * ?????????????????????????????????
     *
     * @param dataCacheVar ??????
     * @return
     */
    protected DimIndex buildIndex(AbstractMemoryTable dataCacheVar) {
        DimIndex dimIndex = new DimIndex(this.indexs);
        dimIndex.buildIndex(dataCacheVar);
        return dimIndex;
    }

    /**
     * ????????????????????????????????????????????????????????????????????????????????????????????? ????????????????????????????????????????????????????????????????????????
     */
    private static ICache<String, IndexExecutor> cache = new SoftReferenceCache<>();

    /**
     * ?????????????????????????????????????????????????????????????????????????????????.
     *
     * @param expressionStr ?????????
     * @param msg           ??????
     * @return ???????????????????????????
     */
    public Map<String, Object> matchExpression(String expressionStr, JSONObject msg) {
        List<Map<String, Object>> rows = matchExpression(expressionStr, msg, true, null);
        if (rows != null && rows.size() > 0) {
            return rows.get(0);
        }
        return null;
    }

    /**
     * ?????????????????????????????????????????????????????????????????????????????????
     *
     * @param expressionStr ?????????
     * @param msg           ??????
     * @return ????????????????????????
     */
    public List<Map<String, Object>> matchExpression(String expressionStr, JSONObject msg, boolean needAll,
        String script) {
        IndexExecutor indexNamelistExecutor = cache.get(expressionStr);
        if (indexNamelistExecutor == null) {
            indexNamelistExecutor = new IndexExecutor(expressionStr, getNameSpace(), this.indexs, dataCache.getCloumnName2DatatType().keySet());
            cache.put(expressionStr, indexNamelistExecutor);
        }
        if (indexNamelistExecutor.isSupport()) {
            return indexNamelistExecutor.match(msg, this, needAll, script);
        } else {
            return matchExpressionByLoop(dataCache.rowIterator(), expressionStr, msg, needAll, script, columnNames);
        }
    }

    /**
     * ????????????????????????????????????????????????????????????
     *
     * @param expressionStr
     * @param msg
     * @param needAll
     * @return
     */
    protected List<Map<String, Object>> matchExpressionByLoop(String expressionStr, JSONObject msg, boolean needAll) {
        AbstractMemoryTable dataCache = this.dataCache;
        List<Map<String, Object>> rows = matchExpressionByLoop(dataCache.rowIterator(), expressionStr, msg, needAll, null, columnNames);
        return rows;
    }

    /**
     * ???????????????????????????????????????????????????????????????join????????????
     *
     * @param expressionStr
     * @param msg
     * @param needAll
     * @return
     */
    public static List<Map<String, Object>> matchExpressionByLoop(Iterator<Map<String, Object>> it,
        String expressionStr, JSONObject msg, boolean needAll) {
        return matchExpressionByLoop(it, expressionStr, msg, needAll, null, new HashSet<>());
    }

    /**
     * ???????????????????????????????????????????????????????????????join????????????
     *
     * @param expressionStr
     * @param msg
     * @param needAll
     * @return
     */
    public static List<Map<String, Object>> matchExpressionByLoop(Iterator<Map<String, Object>> it,
        String expressionStr, JSONObject msg, boolean needAll, String script, Set<String> colunmNames) {
        List<Map<String, Object>> rows = new ArrayList<>();
        Rule ruleTemplete = ExpressionBuilder.createRule("tmp", "tmpRule", expressionStr);
        while (it.hasNext()) {
            Map<String, Object> oldRow = it.next();
            Map<String, Object> newRow = isMatch(ruleTemplete, oldRow, msg, script, colunmNames);
            if (newRow != null) {
                rows.add(newRow);
                if (!needAll) {
                    return rows;
                }
            }
        }
        return rows;
    }

    /**
     * ?????????????????????????????????????????????????????????????????????????????????
     *
     * @param ruleTemplete
     * @param dimRow
     * @param msgRow
     * @param script
     * @param colunmNames
     * @return
     */
    public static Map<String, Object> isMatch(Rule ruleTemplete, Map<String, Object> dimRow, JSONObject msgRow,
        String script, Set<String> colunmNames) {
        Map<String, Object> oldRow = dimRow;
        Map<String, Object> newRow = executeScript(oldRow, script);
        if (ruleTemplete == null) {
            return newRow;
        }
        Rule rule = ruleTemplete.copy();
        Map<String, Expression> expressionMap = new HashMap<>();
        String dimAsName = null;
        ;
        for (Expression expression : rule.getExpressionMap().values()) {
            expressionMap.put(expression.getConfigureName(), expression);
            if (expression instanceof RelationExpression) {
                continue;
            }
            Object object = expression.getValue();
            if (object != null && DataTypeUtil.isString(object.getClass())) {
                String fieldName = (String) object;
                Object value = newRow.get(fieldName);
                if (value != null) {
                    Expression e = expression.copy();
                    e.setValue(value.toString());
                    expressionMap.put(e.getConfigureName(), e);
                }
            }
            if (expression.getVarName().contains(".")) {
                String[] values = expression.getVarName().split("\\.");
                if (values.length == 2) {
                    String asName = values[0];
                    String varName = values[1];
                    if (colunmNames.contains(varName)) {
                        dimAsName = asName;
                    }
                }

            }
        }
        rule.setExpressionMap(expressionMap);
        rule.initElements();
        JSONObject copyMsg = msgRow;
        if (StringUtil.isNotEmpty(dimAsName)) {
            copyMsg = new JSONObject(msgRow);
            for (String key : newRow.keySet()) {
                copyMsg.put(dimAsName + "." + key, newRow.get(key));
            }
        }
        boolean matched = rule.execute(copyMsg);
        if (matched) {
            return newRow;
        }
        return null;
    }

    public static interface IDimField {
        boolean isDimField(Object fieldName);
    }

    /**
     * ??????join??????????????????
     */
    public void createIndexByJoinCondition(String expressionStr, IDimField dimField) {
        List<Expression> expressions = new ArrayList<>();
        List<RelationExpression> relationExpressions = new ArrayList<>();
        Expression expression = ExpressionBuilder.createOptimizationExpression("tmp", "tmp", expressionStr, expressions, relationExpressions);

        RelationExpression relationExpression = null;
        if (expression instanceof RelationExpression) {
            relationExpression = (RelationExpression) expression;
            if (!"and".equals(relationExpression.getRelation())) {
                return;
            }
        }

        List<Expression> indexExpressions = new ArrayList<>();
        List<Expression> otherExpressions = new ArrayList<>();
        if (relationExpression != null) {
            Map<String, Expression> map = new HashMap<>();
            for (Expression tmp : expressions) {
                map.put(tmp.getConfigureName(), tmp);
            }
            for (Expression tmp : relationExpressions) {
                map.put(tmp.getConfigureName(), tmp);
            }
            List<String> expressionNames = relationExpression.getValue();
            relationExpression.setValue(new ArrayList<>());
            for (String expressionName : expressionNames) {
                Expression subExpression = map.get(expressionName);
                if (subExpression != null && !RelationExpression.class.isInstance(subExpression) && dimField.isDimField(subExpression.getValue())) {
                    indexExpressions.add(subExpression);
                } else {
                    otherExpressions.add(subExpression);
                    relationExpression.getValue().add(subExpression.getConfigureName());
                }
            }

        } else {
            indexExpressions.add(expression);
        }

        List<String> fieldNames = new ArrayList<>();

        for (Expression expre : indexExpressions) {
            if (expre instanceof RelationExpression) {
                continue;
            }
            String indexName = expre.getValue().toString();
            if (Equals.isEqualFunction(expre.getFunctionName()) && dimField.isDimField(expre.getValue())) {

                fieldNames.add(indexName);

            }
        }

        String[] indexFieldNameArray = new String[fieldNames.size()];
        int i = 0;
        for (String fieldName : fieldNames) {
            indexFieldNameArray[i] = fieldName;
            i++;
        }
        Arrays.sort(indexFieldNameArray);
        String index = MapKeyUtil.createKey(indexFieldNameArray);
        if (this.getIndexs().contains(index)) {
            return;
        }
        if (indexFieldNameArray.length > 0) {
            this.addIndex(indexFieldNameArray);
        }
    }

    protected static Map<String, Object> executeScript(Map<String, Object> oldRow, String script) {
        if (script == null) {
            return oldRow;
        }
        ScriptComponent scriptComponent = ScriptComponent.getInstance();
        JSONObject msg = new JSONObject();
        msg.putAll(oldRow);
        scriptComponent.getService().executeScript(msg, script);
        return msg;
    }

    protected AbstractMemoryTable loadData() {
        AbstractMemoryTable memoryTable = null;
        if (!isLarge) {
            LOG.info(String.format("init ByteArrayMemoryTable."));
            memoryTable = new ByteArrayMemoryTable();
            loadData2Memory(memoryTable);
        } else {
            LOG.info(String.format("init MappedByteBufferTable."));
            Date date = new Date();
            try {
                memoryTable = MappedByteBufferTable.Creator.newCreator(filePath, date, pollingTimeMinute.intValue()).create(table -> loadData2Memory(table));
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        return memoryTable;
    }

    protected abstract void loadData2Memory(AbstractMemoryTable table);

    @Override
    public void destroy() {
        super.destroy();
        executorService.shutdown();
    }

    /**
     * ????????????
     *
     * @param indexs ???????????????????????????";"??????
     */
    public void setIndex(String indexs) {
        if (StringUtil.isEmpty(indexs)) {
            return;
        }
        List<String> tmp = new ArrayList<>();
        String[] values = indexs.split(";");
        this.addIndex(tmp, values);
        this.indexs = tmp;
    }

    /**
     * ??????????????????????????????????????????map?????????????????????????????????????????????
     *
     * @param fieldNames
     */
    private String addIndex(List<String> indexs, String... fieldNames) {
        if (fieldNames == null) {
            return null;
        }
        Arrays.sort(fieldNames);
        String index = MapKeyUtil.createKey(fieldNames);
        if (StringUtil.isNotEmpty(index)) {
            indexs.add(index);
        }
        return index;
    }

    public Long getPollingTimeMinute() {
        return pollingTimeMinute;
    }

    public void setPollingTimeMinute(Long pollingTimeMinute) {
        this.pollingTimeMinute = pollingTimeMinute;
    }

    public List<String> getIndexs() {
        return indexs;
    }

    public void setIndexs(List<String> indexs) {
        this.indexs = indexs;
    }

    public AbstractMemoryTable getDataCache() {
        return dataCache;
    }

    public boolean isLarge() {
        return isLarge;
    }

    public void setLarge(boolean large) {
        isLarge = large;
    }

    public DimIndex getNameListIndex() {
        return nameListIndex;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }
}
