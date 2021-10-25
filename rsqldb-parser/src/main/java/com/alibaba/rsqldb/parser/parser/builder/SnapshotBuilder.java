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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.rocketmq.streams.dim.intelligence.AbstractIntelligenceCache;
import org.apache.rocketmq.streams.dim.intelligence.AccountIntelligenceCache;
import org.apache.rocketmq.streams.dim.intelligence.DomainIntelligenceCache;
import org.apache.rocketmq.streams.dim.intelligence.IPIntelligenceCache;
import org.apache.rocketmq.streams.dim.intelligence.URLIntelligenceCache;
import org.apache.rocketmq.streams.dim.model.DBDim;
import org.apache.rocketmq.streams.filter.builder.ExpressionBuilder;

import org.apache.rocketmq.streams.filter.operator.Rule;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;
import org.apache.rocketmq.streams.filter.operator.expression.RelationExpression;
import org.apache.rocketmq.streams.filter.operator.expression.SimpleExpression;
import org.apache.rocketmq.streams.common.configure.ConfigureFileKey;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.metadata.MetaDataField;
import org.apache.rocketmq.streams.common.utils.ContantsUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.operator.impl.ScriptOperator;
import com.alibaba.rsqldb.parser.parser.SQLParserContext;
import com.alibaba.rsqldb.parser.parser.SQLNodeParserFactory;
import com.alibaba.rsqldb.parser.parser.ISqlParser;
import com.alibaba.rsqldb.parser.parser.namecreator.ParserNameCreator;
import com.alibaba.rsqldb.parser.parser.result.IParseResult;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlCreateTable.IndexWrapper;
import org.apache.rocketmq.streams.db.driver.JDBCDriver;

/**
 * dimension table join builder specially for url, ip and domain
 */
public class SnapshotBuilder extends SelectSQLBuilder {

    protected static Map<String, AbstractIntelligenceCache> INTELLIGENCE = new HashMap<>();

    protected String expression;
    protected String joinType;
    protected SqlNode expressionSQLNode;

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
    public void buildSQL() {
        CreateSQLBuilder builder = SQLCreateTables.getInstance().get().get(getTableName());
        Properties properties = builder.createProperty();

        String cacheTTLMs = properties.getProperty("cacheTTLMs");
        String tableName = properties.getProperty("tableName");
        long pollingTime = 30;//默认更新时间是30分钟
        if (StringUtil.isNotEmpty(cacheTTLMs)) {
            pollingTime = (Long.valueOf(cacheTTLMs) / 1000 / 60);
        }

        AbstractIntelligenceCache intelligenceCache = INTELLIGENCE.get(tableName);
        if (intelligenceCache != null) {
            buildIntelligence(pollingTime, intelligenceCache);
        } else {
            buildDim(pollingTime, tableName, builder, properties);
        }
    }

    /**
     * 除了情报外的小维表，用通用的逻辑实现
     *
     * @param pollingTime 定时加载时间
     * @param tableName   表名
     * @param builder     维表的创表语句
     * @param properties  维表的with属性
     */
    protected void buildDim(long pollingTime, String tableName, CreateSQLBuilder builder, Properties properties) {
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
        /**
         * 增加索引
         */
        int primaryIndexCount = addPrimaryIndex(builder, dbNameList);
        int indexUniqueCount = addUniqueIndex(builder, dbNameList);
        AtomicBoolean isUnique = new AtomicBoolean(false);
        int indexCount = addIndex(builder, dbNameList, isUnique);

        /**
         * 如果没配置索引，直接抛出错误
         */
        if (primaryIndexCount == 0 && indexCount == 0 && indexUniqueCount == 0) {
            throw new RuntimeException("expect configue index,but not " + builder.getSqlNode().toString());
        }
        /**
         * 只有一个索引，且索引是primary或unique时，可以设置uniqueIndex的值，数据会采用压缩存储
         */
        if (indexCount == 0 && (primaryIndexCount + indexUniqueCount) == 1) {
            dbNameList.setUniqueIndex(true);
        }
        if (indexCount == 1 && primaryIndexCount == 0 && indexUniqueCount == 0 && isUnique.get()) {
            dbNameList.setUniqueIndex(true);
        }

        String selectFields = createSelectFields(builder);
        String sql = "select " + selectFields + " from " + tableName + " limit 1000000";
        if (tableName.trim().toLowerCase().startsWith("from")) {
            sql = "select " + selectFields + " " + tableName + " limit 1000000";
        }

        dbNameList.setSql(sql);
        dbNameList.setSupportBatch(true);

        /**
         * 这里会自动给对象增加namespace，name
         */
        Set<String> fieldNames = new HashSet<>();
        if (selectFields != null) {
            String[] fields = selectFields.split(",");
            for (String field : fields) {
                fieldNames.add(field.trim());
            }
        }

        getPipelineBuilder().addConfigurables(dbNameList);
        JoinConditionSQLBuilder conditionSQLBuilder = new JoinConditionSQLBuilder(fieldNames, asName);
        String expression = convertExpression(conditionSQLBuilder, selectFields, this.getAsName(), selectFields);
        String namespace = dbNameList.getNameSpace();
        String name = dbNameList.getConfigureName();

        List<String> scriptValue = conditionSQLBuilder.getScripts();
        String dimScript = conditionSQLBuilder.getDimScriptValue();
        if (dimScript == null) {
            dimScript = "";
        }
        String script = null;
        if (joinType.toUpperCase().equals("INNER")) {
            String dim = ParserNameCreator.createName("inner_join");
            script = dim + "=inner_join('" + namespace + "','" + name + "','" + expression + "'," + getAsName() + ",'" + dimScript + "'," + selectFields + ");splitArray('" + dim + "');";
        } else if (joinType.toUpperCase().equals("LEFT")) {
            String dim = ParserNameCreator.createName("left_join");
            script = dim + "=left_join('" + namespace + "','" + name + "','" + expression + "'," + getAsName() + ",'" + dimScript + "'," + selectFields + ");if(!null(" + dim + ")){splitArray('" + dim + "');};";
        }
        scriptValue.add(script);
        getPipelineBuilder().addChainStage(new ScriptOperator(conditionSQLBuilder.createScript(scriptValue)));
    }

    /**
     * 创建情报维表，和对应的脚本。情报的表达式，支持等值操作
     *
     * @param pollingTime       多长时间加载一次
     * @param intelligenceCache 情报对应的对象
     */
    protected void buildIntelligence(long pollingTime, AbstractIntelligenceCache intelligenceCache) {
        /**
         * 创建维表连接对象， 默认情报的数据连接是单独配置好的，不依赖sql中create语句
         */
        JDBCDriver dbChannel = new JDBCDriver();
        dbChannel.setUrl(ConfigureFileKey.INTELLIGENCE_JDBC_URL);
        dbChannel.setPassword(ConfigureFileKey.INTELLIGENCE_JDBC_PASSWORD);
        dbChannel.setUserName(ConfigureFileKey.INTELLIGENCE_JDBC_USERNAME);
        getPipelineBuilder().addConfigurables(dbChannel);

        AbstractIntelligenceCache intelligence = ReflectUtil.forInstance(intelligenceCache.getClass());
        intelligence.setDatasourceName(dbChannel.getConfigureName());
        intelligence.setPollingTimeMintue(pollingTime);
        String intelligenceKey = null;
        Rule rule = ExpressionBuilder.createRule("tmp", "tmp", this.expression);
        if (rule.getExpressionMap().size() > 1) {
            throw new RuntimeException(
                "can not support expression in intelligence . the expression is " + expression);
        }
        Expression expression = rule.getExpressionMap().values().iterator().next();
        if (!SimpleExpression.class.isInstance(expression)) {
            throw new RuntimeException(
                "can not support expression in intelligence . the expression is " + expression);
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
            getPipelineBuilder().addChainStage(new ScriptOperator(
                "intelligence('" + intelligence.getNameSpace() + "','" + intelligence.getConfigureName() + "',"
                    + intelligenceKey + ",'" + getAsName() + "');"));
        } else if (joinType.toUpperCase().equals("LEFT")) {
            getPipelineBuilder().addChainStage(new ScriptOperator(
                "left_join_intelligence('" + intelligence.getNameSpace() + "','" + intelligence.getConfigureName() + "',"
                    + intelligenceKey + ",'" + getAsName() + "');"));
        }

    }

    /**
     * 增加主键索引
     *
     * @param builder
     * @param dbNameList
     */
    protected int addPrimaryIndex(CreateSQLBuilder builder, DBDim dbNameList) {

        SqlCreateTable sqlNode = (SqlCreateTable)builder.getSqlNode();
        SqlNodeList sqlNodeList = sqlNode.getPrimaryKeyList();
        if (sqlNodeList == null) {
            return 0;
        }
        String index = createIndexFromSqlNodeList(builder, sqlNodeList);
        if (StringUtil.isNotEmpty(index)) {
            dbNameList.addIndex(index);
        }
        return 1;
    }

    /**
     * 增加主键索引
     *
     * @param builder
     * @param dbNameList
     * @return 增加了几个索引
     */
    protected int addUniqueIndex(CreateSQLBuilder builder, DBDim dbNameList) {

        SqlCreateTable sqlNode = (SqlCreateTable)builder.getSqlNode();
        if (sqlNode == null) {
            return 0;
        }
        List<SqlNodeList> sqlNodeLists = sqlNode.getUniqueKeysList();
        if (sqlNodeLists == null) {
            return 0;
        }
        int count = sqlNodeLists.size();
        for (SqlNodeList sqlNodeList : sqlNodeLists) {
            String index = createIndexFromSqlNodeList(builder, sqlNodeList);
            if (StringUtil.isNotEmpty(index)) {
                dbNameList.addIndex(index);
            }
        }

        return count;
    }

    /**
     * 增加主键索引
     *
     * @param builder
     * @param dbNameList
     * @return 增加了几个索引
     */
    protected int addIndex(CreateSQLBuilder builder, DBDim dbNameList, AtomicBoolean isUnique) {

        SqlCreateTable sqlNode = (SqlCreateTable)builder.getSqlNode();
        if (sqlNode == null) {
            return 0;
        }
        List<IndexWrapper> indexWrappers = sqlNode.getIndexKeysList();
        if (indexWrappers == null) {
            return 0;
        }
        int count = indexWrappers.size();
        for (IndexWrapper indexWrapper : indexWrappers) {
            if (indexWrapper.unique) {
                isUnique.set(true);
            }

            String index = createIndexFromSqlNodeList(builder, indexWrapper.indexKeys);
            if (StringUtil.isNotEmpty(index)) {
                dbNameList.addIndex(index);
            }
        }

        return count;
    }

    /**
     * 根据sqlnodelist 解析出索引信息
     *
     * @param builder
     * @param sqlNodeList
     * @return
     */
    protected String createIndexFromSqlNodeList(CreateSQLBuilder builder, SqlNodeList sqlNodeList) {
        List<String> list = new ArrayList<>();
        for (SqlNode node : sqlNodeList.getList()) {
            IParseResult result = SQLNodeParserFactory.getParse(node).parse(builder, node);
            list.add(result.getReturnValue());

        }
        String index = MapKeyUtil.createKey(";", list);
        return index;
    }

    /**
     * 根据字段名，创建sql，最大加载10 w条数据，超过10w会被截断
     *
     * @param builder
     * @return
     */
    protected String createSelectFields(CreateSQLBuilder builder) {
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
        Set<String> fieldNames = SQLParserContext.getInstance().get().get(getTableName());
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

    public String getExpression() {
        return expression;
    }

    @Override
    public void setExpression(String expression) {
        this.expression = expression;
    }

    public String getJoinType() {
        return joinType;
    }

    public void setJoinType(String joinType) {
        this.joinType = joinType;
    }

    /**
     * 维表不识别别名，需要做去除。维表join，要求维表字段必须在value字段，如果sql写反了，需要转换过来
     */
    protected String convertExpression(JoinConditionSQLBuilder conditionSQLBuilder, String fieldNames, String aliasName, String selectFields) {

        ISqlParser sqlParser = SQLNodeParserFactory.getParse(this.expressionSQLNode);
        conditionSQLBuilder.switchWhere();
        IParseResult result = sqlParser.parse(conditionSQLBuilder, expressionSQLNode);
        String expressionStr = result.getValueForSubExpression();
        Map<String, String> flags = new HashMap<>();
        expressionStr = ContantsUtil.doConstantReplace(expressionStr, flags, 10000);

        List<Expression> expressions = new ArrayList<>();
        List<RelationExpression> relationExpressions = new ArrayList<>();
        Expression expression = ExpressionBuilder.createOptimizationExpression("tmp", "tmp", expressionStr, expressions, relationExpressions);
        Map<String, Expression> map = new HashMap<>();
        if (StringUtil.isNotEmpty(aliasName)) {
            aliasName = aliasName + ".";
        }
        for (Expression express : expressions) {
            map.put(express.getConfigureName(), express);
            String varName = express.getVarName();
            Object valueObject = express.getValue();
            //如果value不是字符串，不做处理
            if (!express.getDataType().matchClass(String.class)) {
                continue;
            }

            String value = (String)valueObject;
            if (StringUtil.isNotEmpty(aliasName) && value.startsWith(aliasName)) {
                value = value.replace(aliasName, "");

            }
            if (fieldNames.contains(value)) {
                express.setValue(value);
                continue;
            }

            if (conditionSQLBuilder.containsFieldName(value)) {
                express.setValue(value);
                continue;
            }

            /***
             *
             */
            if (StringUtil.isNotEmpty(aliasName) && varName.startsWith(aliasName)) {
                varName = varName.replace(aliasName, "");

            }
            if (fieldNames.contains(varName)) {
                express.setVarName(value);
                express.setValue(varName);
                continue;
            }
            if (conditionSQLBuilder.containsFieldName(varName)) {
                express.setVarName(value);
                express.setValue(varName);
                continue;
            }
            throw new RuntimeException("can not parser expression " + expressionStr);

        }
        for (Expression relation : relationExpressions) {
            map.put(relation.getConfigureName(), relation);
        }
        if (flags != null && flags.size() > 0) {
            Iterator<Entry<String, String>> it = flags.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, String> entry = it.next();
                String varName = entry.getKey();
                String value = entry.getValue();
                conditionSQLBuilder.addScript(varName + "=" + value + ";");
            }
        }
        return expression.toExpressionString(map);
    }

    public SqlNode getExpressionSQLNode() {
        return expressionSQLNode;
    }

    public void setExpressionSQLNode(SqlNode expressionSQLNode) {
        this.expressionSQLNode = expressionSQLNode;
    }
}
