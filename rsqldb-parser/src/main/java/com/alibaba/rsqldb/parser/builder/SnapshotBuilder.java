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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.alibaba.rsqldb.parser.SqlBuilderResult;
import com.alibaba.rsqldb.parser.creator.ParserNameCreator;
import com.alibaba.rsqldb.parser.sql.context.CreateSqlBuildersForSqlTreeContext;
import com.alibaba.rsqldb.parser.sql.context.FieldsOfTableForSqlTreeContext;

import org.apache.rocketmq.streams.common.configuration.ConfigurationKey;
import org.apache.rocketmq.streams.common.interfaces.IDim;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.metadata.MetaDataField;
import org.apache.rocketmq.streams.common.model.NameCreatorContext;
import org.apache.rocketmq.streams.common.topology.stages.DimChainStage;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.db.driver.JDBCDriver;
import org.apache.rocketmq.streams.dim.builder.IDimSQLParser;
import org.apache.rocketmq.streams.dim.builder.SQLParserFactory;
import org.apache.rocketmq.streams.dim.intelligence.AbstractIntelligenceCache;
import org.apache.rocketmq.streams.dim.intelligence.AccountIntelligenceCache;
import org.apache.rocketmq.streams.dim.intelligence.DomainIntelligenceCache;
import org.apache.rocketmq.streams.dim.intelligence.IPIntelligenceCache;
import org.apache.rocketmq.streams.dim.intelligence.URLIntelligenceCache;
import org.apache.rocketmq.streams.dim.model.AbstractDim;
import org.apache.rocketmq.streams.dim.model.DBDim;
import org.apache.rocketmq.streams.dim.model.FileDim;
import org.apache.rocketmq.streams.filter.builder.ExpressionBuilder;
import org.apache.rocketmq.streams.filter.function.expression.Equals;
import org.apache.rocketmq.streams.filter.operator.Rule;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;
import org.apache.rocketmq.streams.filter.operator.expression.RelationExpression;
import org.apache.rocketmq.streams.filter.operator.expression.SimpleExpression;
import org.apache.rocketmq.streams.script.operator.impl.ScriptOperator;

/**
 * dimension table join builder specially for url, ip and domain
 */
public class SnapshotBuilder extends SelectSqlBuilder {

    protected static Map<String, AbstractIntelligenceCache> INTELLIGENCE = new HashMap<>();
    protected static Map<CreateSqlBuilder, IDim> dims = new HashMap<>();

    static {
        DomainIntelligenceCache domainIntelligenceCache = new DomainIntelligenceCache();
        INTELLIGENCE.put(domainIntelligenceCache.getTableName(), domainIntelligenceCache);
        IPIntelligenceCache ipIntelligenceCache = new IPIntelligenceCache();
        INTELLIGENCE.put(ipIntelligenceCache.getTableName(), ipIntelligenceCache);
        URLIntelligenceCache urlIntelligenceCache = new URLIntelligenceCache();
        INTELLIGENCE.put(urlIntelligenceCache.getTableName(), urlIntelligenceCache);
        AccountIntelligenceCache accountIntelligenceCache = new AccountIntelligenceCache();
        INTELLIGENCE.put(accountIntelligenceCache.getTableName(), accountIntelligenceCache);
    }

    @Override
    public SqlBuilderResult buildSql() {
        throw new RuntimeException("can not support this method, please use buildDimCondition");
    }

    /**
     * 在join sql build中调用，一个sql如果有对一个维表多次join，数据只存一份。通过建立不同索引实现
     *
     * @param condition
     * @param joinType
     */
    public void buildDimCondition(String condition, String joinType) {

        CreateSqlBuilder builder = CreateSqlBuildersForSqlTreeContext.getInstance().get().get(getTableName());
        Properties properties = builder.getProperties();

        String cacheTTLMs = properties.getProperty("cacheTTLMs");
        String tableName = properties.getProperty("tableName");
        long pollingTime = 30;//默认更新时间是30分钟

        if (StringUtil.isNotEmpty(cacheTTLMs)) {
            long ms = Long.valueOf(cacheTTLMs);
            if (ms < 1000 * 60) {
                ms = 1000 * 60;
            }
            pollingTime = (Long.valueOf(ms) / 1000 / 60);
        }
        /**
         * 阿里内部使用，如果是情报类的，直接个性化加载
         */

        IDim dim = dims.get(builder);
        /**
         * 通用维表builder
         */
        if (dim == null) {
            dim = buildDim(builder, properties);
            String dimName = NameCreatorContext.get().createOrGet(this.getPipelineBuilder().getPipelineName()).createName(this.getPipelineBuilder().getPipelineName(), DBDim.TYPE);
            dim.setName(dimName);
            getPipelineBuilder().addConfigurables(dim);
            dims.put(builder, dim);
        }

        /**
         * 这里会自动给对象增加namespace，name
         */
        String selectFields = createSelectFields(builder);
        Set<String> fieldNames = createFieldNames(selectFields);
        JoinConditionSqlBuilder conditionSQLBuilder = new JoinConditionSqlBuilder(builder, fieldNames, asName);
        String expressionStr = conditionSQLBuilder.parseConditionSQL(condition);

        //编译join条件
        createIndexByJoinCondition(dim, expressionStr, builder);
        addDimJob2Pipeline(dim, conditionSQLBuilder, expressionStr, joinType, selectFields);

    }

    private String getDimType(Properties properties) {
        String type = properties.getProperty("type");
        if (StringUtil.isEmpty(type)) {
            type = properties.getProperty("TYPE");
        }
        if (StringUtil.isEmpty(type)) {
            type = properties.getProperty("connector");
        }
        if (StringUtil.isEmpty(type)) {
            type = properties.getProperty("CONNECTOR");
        }
        return type;
    }

    /**
     * 创建dim对象，相同的create table ，只创建一个对象
     *
     * @param builder
     * @param properties
     * @return
     */
    protected IDim buildDim(CreateSqlBuilder builder, Properties properties) {
        String type = getDimType(properties).toLowerCase();
        IDimSQLParser dimSQLParser = SQLParserFactory.getInstance().create(type);
        return dimSQLParser.parseDim(namespace, properties, builder.getMetaData());
    }

    /**
     * 创建dim对象，相同的create table ，只创建一个对象
     *
     * @param pollingTime
     * @param tableName
     * @param builder
     * @param properties
     * @return
     */
    protected AbstractDim buildDBDim(long pollingTime, String tableName, CreateSqlBuilder builder,
        Properties properties) {
        /**
         * 创建namelist，要起必须有pco rimary key，，否则抛出错误
         */
        String url = properties.getProperty("url");
        String userName = properties.getProperty("userName");
        String password = properties.getProperty("password");

        DBDim dbNameList = new DBDim();
        dbNameList.setUrl(url);
        dbNameList.setUserName(userName);
        dbNameList.setPassword(password);
        dbNameList.setPollingTimeSeconds(pollingTime);

        String selectFields = createSelectFields(builder);
        String sql = "select " + selectFields + " from " + tableName + " limit 1000000";
        if (tableName.trim().toLowerCase().startsWith("from")) {
            sql = "select " + selectFields + " " + tableName + " limit 1000000";
        }

        dbNameList.setSql(sql);
        dbNameList.setSupportBatch(true);

        return dbNameList;
    }

    protected AbstractDim buildFileDim(long time, String name, CreateSqlBuilder builder, Properties properties) {
        String filePath = properties.getProperty("filePath");
        if (StringUtil.isEmpty(filePath)) {
            filePath = properties.getProperty("file_path");
        }
        FileDim fileDim = new FileDim();
        fileDim.setFilePath(filePath);
        return fileDim;
    }

    /**
     * 根据join条件设置索引
     *
     * @param dbNameList
     */
    protected void createIndexByJoinCondition(IDim dbNameList, String expressionStr,
        CreateSqlBuilder createSQLBuilder) {
        if (StringUtil.isEmpty(expressionStr)) {
            return;
        }
        List<Expression> expressions = new ArrayList<>();
        List<RelationExpression> relationExpressions = new ArrayList<>();
        Expression expression = ExpressionBuilder.createOptimizationExpression("tmp", "tmp", expressionStr, expressions, relationExpressions);

        RelationExpression relationExpression = null;
        if (RelationExpression.class.isInstance(expression)) {
            relationExpression = (RelationExpression)expression;
            if (!"and".equals(relationExpression.getRelation())) {
                return;
            }
        }

        List<Expression> indexExpressions = new ArrayList<>();
        //  List<Expression> otherExpressions = new ArrayList<>();
        if (relationExpression != null) {
            Map<String, Expression> map = new HashMap<>();
            for (Expression tmp : expressions) {
                map.put(tmp.getName(), tmp);
            }
            for (Expression tmp : relationExpressions) {
                map.put(tmp.getName(), tmp);
            }
            List<String> expressionNames = relationExpression.getValue();
            relationExpression.setValue(new ArrayList<>());
            for (String expressionName : expressionNames) {
                Expression subExpression = map.get(expressionName);
                if (subExpression != null && !RelationExpression.class.isInstance(subExpression) && isDimField(subExpression.getValue(), createSQLBuilder)) {
                    indexExpressions.add(subExpression);
                } else {
                    relationExpression.getValue().add(expressionName);
                }
            }

        } else {
            indexExpressions.add(expression);
        }

        List<String> fieldNames = new ArrayList<>();

        for (Expression expre : indexExpressions) {
            if (RelationExpression.class.isInstance(expre)) {
                continue;
            }
            String indexName = expre.getValue().toString();
            if (Equals.isEqualFunction(expre.getFunctionName()) && isDimField(expre.getValue(), createSQLBuilder)) {

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
        if (dbNameList.getIndexs().contains(index)) {
            return;
        }
        if (indexFieldNameArray.length > 0) {
            dbNameList.addIndex(indexFieldNameArray);
        }
    }

    protected boolean isDimField(Object value, CreateSqlBuilder createSQLBuilder) {
        if (!String.class.isInstance(value)) {
            return false;
        }
        MetaData metaData = createSQLBuilder.getMetaData();
        if (metaData.getMetaDataField((String)value) != null) {
            return true;
        }
        return false;
    }

    /**
     * 生成dim的脚本，并设置pipeline的stage
     *
     * @param dbNameList
     * @param conditionSQLBuilder
     * @param expression
     * @param joinType
     * @param selectFields
     */
    protected void addDimJob2Pipeline(IDim dbNameList, JoinConditionSqlBuilder conditionSQLBuilder,
        String expression, String joinType, String selectFields) {
        List<String> scriptValue = conditionSQLBuilder.getScripts();
        String dimScript = conditionSQLBuilder.getDimScriptValue();
        if (dimScript == null) {
            dimScript = "";
        }
        boolean isInnerJoin = false;
        boolean isLeftJoin = false;
        String script = null;
        String dim = null;
        if (joinType.toUpperCase().equals("INNER")) {
            isInnerJoin = true;
            dim = ParserNameCreator.createName("inner_join");
            script = "splitArray('" + dim + "');";
        } else if (joinType.toUpperCase().equals("LEFT")) {
            isLeftJoin = true;
            dim = ParserNameCreator.createName("left_join");
            script = "if(!null(" + dim + ")){splitArray('" + dim + "');};";
        }
        scriptValue.add(script);
        DimChainStage chainStage = new DimChainStage();
        chainStage.setExpressionStr(expression);
        chainStage.setAlias(getAsName());
        chainStage.setDimScript(dimScript);
        chainStage.setSplitFieldName(dim);
        chainStage.setInnerJoin(isInnerJoin);
        chainStage.setLeftJoin(isLeftJoin);
        ScriptOperator scriptOperator = new ScriptOperator(conditionSQLBuilder.createScript(scriptValue));
        chainStage.setScript(scriptOperator);
        chainStage.setDim(dbNameList);
        chainStage.setDiscription("Dim Join");
        getPipelineBuilder().addChainStage(chainStage);
        getPipelineBuilder().addConfigurables(scriptOperator);
        String sql = joinType + " " + conditionSQLBuilder.getBuilder().getTableName();
        if (conditionSQLBuilder.getSqlNode() != null) {
            sql = sql + " on " + conditionSQLBuilder.getSqlNode().toString();
        }
        chainStage.setSql(sql);
    }

    private Set<String> createFieldNames(String selectFields) {
        Set<String> fieldNames = new HashSet<>();
        if (selectFields != null) {
            String[] fields = selectFields.split(",");
            for (String field : fields) {
                fieldNames.add(field.trim());
            }
        }
        return fieldNames;
    }

    /**
     * 创建情报维表，和对应的脚本。情报的表达式，支持等值操作
     *
     * @param pollingTime       多长时间加载一次
     * @param intelligenceCache 情报对应的对象
     */
    protected void buildIntelligence(long pollingTime, AbstractIntelligenceCache intelligenceCache, String joinType,
        String expressionStr) {
        /**
         * 创建维表连接对象， 默认情报的数据连接是单独配置好的，不依赖sql中create语句
         */
        JDBCDriver dbChannel = new JDBCDriver();
        dbChannel.setUrl(ConfigurationKey.INTELLIGENCE_JDBC_URL);
        dbChannel.setPassword(ConfigurationKey.INTELLIGENCE_JDBC_PASSWORD);
        dbChannel.setUserName(ConfigurationKey.INTELLIGENCE_JDBC_USERNAME);
        getPipelineBuilder().addConfigurables(dbChannel);

        AbstractIntelligenceCache intelligence = ReflectUtil.forInstance(intelligenceCache.getClass());
        intelligence.setOutputDataSource(dbChannel);
        intelligence.setPollingTimeMinute(pollingTime);
        String intelligenceKey = null;
        Rule rule = ExpressionBuilder.createRule("tmp", "tmp", expressionStr);
        if (rule.getExpressionMap().size() > 1) {
            throw new RuntimeException("can not support expression in intelligence . the expression is " + expression);
        }
        Expression expression = rule.getExpressionMap().values().iterator().next();
        if (!SimpleExpression.class.isInstance(expression)) {
            throw new RuntimeException("can not support expression in intelligence . the expression is " + expressionStr);
        }
        if (expression.getVarName().equals(asName + "." + intelligence.getKeyName()) || expression.getVarName().equals(intelligence.getKeyName())) {
            intelligenceKey = expression.getValue().toString();
        } else {
            intelligenceKey = expression.getVarName();
        }
        getPipelineBuilder().addConfigurables(intelligence);
        /**
         *
         */
        if (joinType.toUpperCase().equals("INNER")) {
            getPipelineBuilder().addChainStage(new ScriptOperator("intelligence('" + intelligence.getNameSpace() + "','" + intelligence.getName() + "'," + intelligenceKey + ",'" + getAsName() + "');"));
        } else if (joinType.toUpperCase().equals("LEFT")) {
            getPipelineBuilder().addChainStage(new ScriptOperator("left_join_intelligence('" + intelligence.getNameSpace() + "','" + intelligence.getName() + "'," + intelligenceKey + ",'" + getAsName() + "');"));
        }

    }

    /**
     * 根据字段名，创建sql，最大加载10 w条数据，超过10w会被截断
     *
     * @param builder
     * @return
     */
    protected String createSelectFields(CreateSqlBuilder builder) {
        MetaData metaData = builder.getMetaData();
        List<MetaDataField> metaDataFields = metaData.getMetaDataFields();
        StringBuilder stringBuilder = new StringBuilder();
        boolean isFirst = true;
        for (MetaDataField field : metaDataFields) {
            if (isFirst) {
                isFirst = false;
            } else {
                stringBuilder.append(",");
            }
            stringBuilder.append(field.getFieldName());
        }
        String fields = stringBuilder.toString();
        return fields;
    }

    /**
     * 如果有别名，必须加别名
     *
     * @param fieldName
     * @return
     */
    @Override
    public String getFieldName(String fieldName) {
        String name = doAllFieldName(fieldName);
        if (name != null) {
            return name;
        }
        Set<String> fieldNames = FieldsOfTableForSqlTreeContext.getInstance().get().get(getTableName());
        String asName = null;
        String fieldValue = fieldName;
        String asNameStr = getAsName() == null ? "" : getAsName() + ".";
        int index = fieldName.indexOf(".");
        if (index != -1) {
            asName = fieldName.substring(0, index);
            fieldValue = fieldName.substring(index + 1);
            if (!asName.equals(getAsName())) {
                return null;
            }
        }
        if (!fieldNames.contains(fieldValue)) {
            return null;
        }
        return asNameStr + fieldValue;
    }

    @Override
    public Set<String> parseDependentTables() {
        Set<String> tables = new HashSet<>();
        tables.add(getTableName());
        return tables;
    }

    @Override
    public Set<String> getAllFieldNames() {
        CreateSqlBuilder builder = CreateSqlBuildersForSqlTreeContext.getInstance().get().get(getTableName());
        Set<String> fields = new HashSet<>();
        List<MetaDataField> metaDataFields = builder.getMetaData().getMetaDataFields();
        for (MetaDataField metaDataField : metaDataFields) {
            fields.add(metaDataField.getFieldName());
        }
        return fields;
    }
}
