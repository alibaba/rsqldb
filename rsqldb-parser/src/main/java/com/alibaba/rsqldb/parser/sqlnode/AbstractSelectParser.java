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
package com.alibaba.rsqldb.parser.sqlnode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.rsqldb.parser.ISqlNodeParser;
import com.alibaba.rsqldb.parser.SqlNodeParserFactory;
import com.alibaba.rsqldb.parser.builder.CreateSqlBuilder;
import com.alibaba.rsqldb.parser.builder.JoinSqlBuilder;
import com.alibaba.rsqldb.parser.builder.SelectSqlBuilder;
import com.alibaba.rsqldb.parser.builder.UnionSqlBuilder;
import com.alibaba.rsqldb.parser.function.TumbleParser;
import com.alibaba.rsqldb.parser.result.BuilderParseResult;
import com.alibaba.rsqldb.parser.result.IParseResult;
import com.alibaba.rsqldb.parser.result.ScriptParseResult;
import com.alibaba.rsqldb.parser.result.VarParseResult;
import com.alibaba.rsqldb.parser.sql.context.CreateSqlBuildersForSqlTreeContext;
import com.alibaba.rsqldb.parser.sql.context.FieldsOfTableForSqlTreeContext;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.metadata.MetaDataField;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.common.utils.DataTypeUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.SQLFormatterUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.filter.builder.ExpressionBuilder;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

public abstract class AbstractSelectParser extends AbstractSelectNodeParser<SqlSelect> {

    private static final Log LOG = LogFactory.getLog(AbstractSelectParser.class);
    private static final UnionParser unionParser = new UnionParser();
    private SQLFormatterUtil sqlFormatterUtil = new SQLFormatterUtil();

    @Override
    public IParseResult parse(SelectSqlBuilder selectSQLBuilder, SqlSelect sqlSelect) {
        selectSQLBuilder.setSqlNode(sqlSelect);
        Properties configuration = selectSQLBuilder.getConfiguration();
        boolean isNeedWhereToCondition = parseFrom(selectSQLBuilder, sqlSelect.getFrom(), null, configuration);
        parseWhere(selectSQLBuilder, sqlSelect, isNeedWhereToCondition, configuration);
        parseGroup(selectSQLBuilder, sqlSelect);
        parseSelect(selectSQLBuilder, sqlSelect, configuration);
        selectSQLBuilder.addDependentTable(getDependentTable(selectSQLBuilder));
        return new BuilderParseResult(selectSQLBuilder);
    }

    protected void parseGroup(SelectSqlBuilder builder, SqlSelect sqlSelect) {
        if (sqlSelect.getGroup() == null) {
            return;
        }
        SqlNodeList groups = sqlSelect.getGroup();
        builder.setGroupSql(sqlFormatterUtil.format("GROUP \n" + groups.toString()));
        List<SqlNode> list = groups.getList();
        if (builder.getWindowBuilder() == null) {
            TumbleParser.createWindowBuilder(builder);
        }
        builder.getWindowBuilder().setConfiguration(builder.getConfiguration());
        for (SqlNode sqlNode : list) {
            IParseResult result = parseSqlNode(builder, sqlNode);
            if (result != null && result.getReturnValue() != null) {
                builder.getWindowBuilder().getGroupByFieldNames().add(result.getReturnValue());
            }
        }
        if (sqlSelect.getHaving() != null) {
            boolean isSelectStage = builder.isSelectStage();
            boolean isFromStage = builder.isFromStage();
            builder.switchWhere();
            SqlNode sqlNode = sqlSelect.getHaving();
            builder.setHavingSql(sqlFormatterUtil.format("HAVING \n" + sqlNode.toString()));
            List<String> scripts = builder.getScripts();
            builder.setScripts(new ArrayList<>());
            boolean isCloseFieldCheck = builder.isCloseFieldCheck();
            builder.setCloseFieldCheck(true);
            String expression = parseSqlNode(builder, sqlNode).getValueForSubExpression();
            List<String> havingScript = builder.getScripts();
            builder.getWindowBuilder().setHavingScript(havingScript);
            builder.setScripts(scripts);
            builder.setCloseFieldCheck(isCloseFieldCheck);
            builder.getWindowBuilder().setHaving(expression);
            if (isSelectStage) {
                builder.switchSelect();
            }
            if (isFromStage) {
                builder.switchFrom();
            }
        }

    }

    /**
     * 如果是嵌套逻辑，则找最底层的表依赖
     *
     * @param tableDescriptor
     * @return
     */
    private String getDependentTable(SelectSqlBuilder tableDescriptor) {
        String tableName = tableDescriptor.getTableName();
        SelectSqlBuilder current = tableDescriptor;
        while (current.getSubSelect() != null) {
            current = current.getSubSelect();
            tableName = current.getTableName();
        }
        return tableName;
    }

    @Override
    public boolean support(Object sqlNode) {
        return sqlNode instanceof SqlSelect;
    }

    /**
     * 主要是解析函数部分，把内置函数转换成脚本函数
     *
     * @param sqlBuilder
     * @param sqlSelect
     */
    protected void parseSelect(SelectSqlBuilder sqlBuilder, SqlSelect sqlSelect, Properties configuration) {
        sqlBuilder.setDistinct(sqlSelect.isDistinct());
        List<String> scripts = sqlBuilder.getScripts();
        sqlBuilder.setScripts(new ArrayList<>());
        sqlBuilder.switchSelect();
        SqlNodeList sqlNodes = sqlSelect.getSelectList();
        sqlBuilder.setSelectSql(sqlFormatterUtil.format("SELECT \n" + sqlNodes.toString()));

        for (SqlNode sqlNode : sqlNodes.getList()) {
            ISqlNodeParser sqlParser = SqlNodeParserFactory.getParse(sqlNode);
            if (sqlParser == null) {
                throw new RuntimeException(sqlNode.toString() + " not have parser to parser,may be have error");
            }

            SelectSqlBuilder builder = sqlBuilder;
            if (sqlBuilder.getWindowBuilder() != null) {

                /**
                 * 期望每一个字段和它所有的脚本放到一起，而不是把所有字段的脚本混在一起
                 */
                SelectSqlBuilder selectSQLBuilder = new SelectSqlBuilder();
                selectSQLBuilder.setTableName(sqlBuilder.getTableName());
                selectSQLBuilder.setAsName(sqlBuilder.getAsName());
                selectSQLBuilder.setUnionSQLBuilder(sqlBuilder.getUnionSQLBuilder());
                selectSQLBuilder.setSubSelect(sqlBuilder.getSubSelect());
                selectSQLBuilder.setWindowBuilder(sqlBuilder.getWindowBuilder());
                selectSQLBuilder.setJoinSQLDescriptor(sqlBuilder.getJoinSQLDescriptor());
                selectSQLBuilder.setConfiguration(configuration);
                builder = selectSQLBuilder;

            }
            IParseResult sqlVar = sqlParser.parse(builder, sqlNode);

            String varName = sqlVar.getReturnValue();

            if (builder.getOverWindowBuilder() != null) {
                sqlBuilder.setOverWindowBuilder(builder.getOverWindowBuilder());
                sqlBuilder.setOverName(builder.getOverName());
                sqlBuilder.putSelectField(varName, sqlVar);
                continue;
            }
            if (StringUtil.isEmpty(varName)) {
                throw new RuntimeException("the field parser error " + sqlNode.toString());
            }
            if (sqlBuilder.getWindowBuilder() != null) {
                List<String> varScripts = builder.getScripts();
                if (CollectionUtil.isNotEmpty(varScripts)) {
                    ScriptParseResult scriptParseResult = createScriptResult(builder);
                    sqlBuilder.getWindowBuilder().getSelectMap().put(varName, scriptParseResult.getScript());
                } else {
                    sqlBuilder.getWindowBuilder().getSelectMap().put(varName, varName);
                }
                sqlBuilder.putSelectField(varName, new VarParseResult(varName));
                continue;
            }
            if (StringUtil.isEmpty(varName)) {
                throw new RuntimeException("the field parser error " + sqlNode.toString());
            }
            sqlBuilder.putSelectField(varName, sqlVar);
        }
        List<String> selectScripts = sqlBuilder.getScripts();
        sqlBuilder.setSelectScripts(selectScripts);
        sqlBuilder.setScripts(scripts);
    }

    protected ScriptParseResult createScriptResult(SelectSqlBuilder builder) {
        ScriptParseResult scriptParseResult = new ScriptParseResult();
        List<String> scriptList = builder.getScripts();
        if (scriptList != null) {
            for (String script : scriptList) {
                scriptParseResult.addScript(script);
            }
        }
        return scriptParseResult;
    }

    /**
     * 如果是嵌套表，则解析嵌套部分，否则解析出表名
     *
     * @param rootSelectSQLBuilder
     * @param from
     */
    protected boolean parseFrom(SelectSqlBuilder rootSelectSQLBuilder, SqlNode from, String aliasName, Properties configuration) {
        rootSelectSQLBuilder.switchFrom();
        //        SqlNode from = sqlSelect.getFrom();
        if (from == null) {
            return false;
        }
        final SqlNode oriFrom = from;
        /**
         * 有 as的场景
         */
        if (from instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall)from;
            if (sqlBasicCall.getOperator().getName().equalsIgnoreCase("as")) {
                from = sqlBasicCall.getOperandList().get(0);
                aliasName = sqlBasicCall.getOperandList().get(1).toString();
            }
            rootSelectSQLBuilder.setFromSql("FROM " + from + " as " + aliasName);
        }

        /**
         * 是子查询的场景
         */
        if (from instanceof SqlSelect) {
            SelectSqlBuilder selectSQLDescriptor = new SelectSqlBuilder();
            selectSQLDescriptor.setParentSelect(rootSelectSQLBuilder);
            selectSQLDescriptor.setSqlNode(from);
            selectSQLDescriptor.setConfiguration(configuration);
            rootSelectSQLBuilder.setSubSelect(selectSQLDescriptor);
            if (StringUtil.isNotEmpty(aliasName)) {
                selectSQLDescriptor.setAsName(aliasName);
            }
            parse(selectSQLDescriptor, (SqlSelect)from);
            rootSelectSQLBuilder.setTableName(selectSQLDescriptor.getTableName());

            if (aliasName != null) {
                rootSelectSQLBuilder.setFromSql("FROM " + aliasName);
            } else {
                rootSelectSQLBuilder.setFromSql("FROM " + "sub_select_table");
            }
            /**
             * join的场景
             */
        } else if (from instanceof SqlJoin) {
            rootSelectSQLBuilder.setSupportOptimization(false);
            JoinSqlBuilder joinSQLBuilder = new JoinSqlBuilder();
            joinSQLBuilder.setTableName2Builders(rootSelectSQLBuilder.getTableName2Builders());
            joinSQLBuilder.setConfiguration(rootSelectSQLBuilder.getConfiguration());
            IParseResult result = SqlNodeParserFactory.getParse(from).parse(joinSQLBuilder, from);
            rootSelectSQLBuilder.setJoinSQLDescriptor(joinSQLBuilder);
            if (result != null) {
                if (joinSQLBuilder.getLeft() != null) {
                    rootSelectSQLBuilder.setTableName(joinSQLBuilder.getLeft().getReturnValue());
                }
            }
            if (StringUtil.isNotEmpty(aliasName)) {
                rootSelectSQLBuilder.setAsName(aliasName);
            }
            rootSelectSQLBuilder.setJoinSQLDescriptor(joinSQLBuilder);
            rootSelectSQLBuilder.setFromSql("FROM " + joinSQLBuilder.createSQLFromParser());
            return joinSQLBuilder.isNeedWhereToCondition();
            /**
             * union all场景
             */
        } else if (unionParser.support(from)) {
            UnionSqlBuilder unionSQLBuilder = unionParser.create(configuration);
            if (StringUtil.isNotEmpty(aliasName)) {
                rootSelectSQLBuilder.setAsName(aliasName);
            }
            unionParser.parse(unionSQLBuilder, (SqlBasicCall)from);
            //            Set<String> tableNames = unionSQLBuilder.parseDependentTables();
            ////            if (tableNames == null || tableNames.size() > 1) {
            ////                throw new RuntimeException("can not support union parser, expect one source。");
            ////            }
            ////            String tableName = tableNames.iterator().next();
            rootSelectSQLBuilder.setTableName(unionSQLBuilder.getTableName());
            rootSelectSQLBuilder.setUnionSQLBuilder(unionSQLBuilder);

            rootSelectSQLBuilder.setFromSql("FROM " + "union(" + MapKeyUtil.createKeyFromCollection(",", unionSQLBuilder.getTableNames()) + ")");
        } else if (from instanceof SqlBasicCall && ((SqlBasicCall)from).getOperator().getName().equalsIgnoreCase("values")) {
            rootSelectSQLBuilder.setTableName(aliasName);
            List<String> fieldNames = new ArrayList<>();
            SqlBasicCall sqlBasicCall = (SqlBasicCall)oriFrom;
            for (int i = 2; i < sqlBasicCall.getOperandList().size(); i++) {
                String fieldName = FunctionUtils.getConstant(sqlBasicCall.getOperandList().get(i).toString());
                fieldNames.add(fieldName);
            }
            parseRows(aliasName, from, rootSelectSQLBuilder, fieldNames, ((SqlBasicCall)from).getOperandList(), configuration);

        } else {//单表名的场景
            rootSelectSQLBuilder.setTableName(from.toString());
            if (StringUtil.isNotEmpty(aliasName)) {
                rootSelectSQLBuilder.setAsName(aliasName);
                rootSelectSQLBuilder.setFromSql(from.toString() + " as " + aliasName);
            } else {
                rootSelectSQLBuilder.setFromSql(from.toString());
            }
            rootSelectSQLBuilder.setFromSql("FROM " + from.toString() + " " + (StringUtil.isNotEmpty(aliasName) ? "as " + aliasName : ""));

            //tableDescriptor.addDependentTable(tableDescriptor.getTableName());
        }
        return false;
    }

    protected void parseRows(String tableName, SqlNode rowSqlNode, SelectSqlBuilder selectSQLBuilder, List<String> fieldNames, List<SqlNode> rowList, Properties configuration) {
        //SQLCreateTables.getInstance().get().
        MetaData metaData = new MetaData();
        metaData.setTableName(tableName);
        boolean isCreateMetaData = false;
        List<String> rows = new ArrayList<>();
        for (SqlNode rowNode : rowList) {
            if (!(rowNode instanceof SqlBasicCall)) {
                throw new RuntimeException("expect row Node, real is " + rowNode.toString());
            }
            SqlBasicCall sqlBasicCall = (SqlBasicCall)rowNode;
            if (!sqlBasicCall.getOperator().getName().equalsIgnoreCase("ROW")) {
                throw new RuntimeException("expect row Node, real is " + rowNode.toString());
            }
            if (fieldNames == null || sqlBasicCall.getOperandList().size() != fieldNames.size()) {
                throw new RuntimeException("expect row Node and equal fieldname size, real is " + sqlBasicCall.getOperandList().size() + ":" + (fieldNames == null ? 0 : fieldNames.size()));
            }

            JSONObject row = new JSONObject();

            for (int i = 0; i < fieldNames.size(); i++) {
                SqlNode sqlNode = sqlBasicCall.getOperandList().get(i);
                Object value = parseSqlNode(selectSQLBuilder, sqlNode).getResultValue();
                row.put(fieldNames.get(i), value);

                if (!isCreateMetaData) {
                    MetaDataField metaDataField = new MetaDataField();
                    metaDataField.setFieldName(fieldNames.get(i));
                    if (value != null) {
                        metaDataField.setDataType(DataTypeUtil.getDataTypeFromClass(value.getClass()));
                    }
                    metaDataField.setIsRequired(false);
                    metaData.getMetaDataFields().add(metaDataField);
                }

            }
            rows.add(row.toString());
            isCreateMetaData = true;

        }
        CreateSqlBuilder createSQLBuilder = new CreateSqlBuilder();
        createSQLBuilder.setTableName(tableName);
        createSQLBuilder.setMetaData(metaData);
        Properties properties = new Properties();
        properties.put("type", "row_list");
        properties.put("rows", rows);
        properties.put("isJsonData", true);
        createSQLBuilder.setProperties(properties);
        createSQLBuilder.setCreateTable(tableName);
        createSQLBuilder.setSqlNode(rowSqlNode);
        createSQLBuilder.setConfiguration(configuration);
        CreateSqlBuildersForSqlTreeContext.getInstance().get().put(tableName, createSQLBuilder);
        FieldsOfTableForSqlTreeContext.getInstance().get().put(tableName, new HashSet<>(fieldNames));
        CreateSqlBuildersForSqlTreeContext.getInstance().getCreateSqlBuilders().add(createSQLBuilder);

    }

    /**
     * 主要是解析where部分，有两种解析，如果有函数，转换成脚本函数，如果是表达式，转换成规则引擎表达式
     *
     * @param selectSQLBuilder
     * @param sqlSelect
     */
    protected void parseWhere(SelectSqlBuilder selectSQLBuilder, SqlSelect sqlSelect, boolean isNeedWhereToCondition, Properties configuration) {
        if (isNeedWhereToCondition) {
            SqlNode sqlNode = sqlSelect.getWhere();
            selectSQLBuilder.setJoinConditionSql(sqlFormatterUtil.format("ON \n" + sqlNode.toString()));
            if (sqlNode == null) {
                throw new RuntimeException("expect join condition from next where, but where is null " + sqlSelect.toString());
            }
            ISqlNodeParser sqlParser = SqlNodeParserFactory.getParse(sqlNode);
            boolean isSelectSwitch = selectSQLBuilder.isSelectStage();
            boolean isFromSwitch = selectSQLBuilder.isFromStage();
            selectSQLBuilder.switchWhere();
            IParseResult result = sqlParser.parse(selectSQLBuilder, sqlNode);
            if (isFromSwitch) {
                selectSQLBuilder.switchFrom();
            }
            if (isSelectSwitch) {
                selectSQLBuilder.switchSelect();
            }
            JoinSqlBuilder joinSQLBuilder = selectSQLBuilder.getJoinSQLDescriptor();
            joinSQLBuilder.setOnCondition(result.getValueForSubExpression());
            //            joinSQLBuilder.setConditionSqlNode(sqlNode);
            joinSQLBuilder.setConfiguration(configuration);
            return;
        }

        selectSQLBuilder.switchWhere();
        SqlNode sqlNode = sqlSelect.getWhere();
        if (sqlNode == null) {
            return;
        }
        selectSQLBuilder.setWhereSql(sqlFormatterUtil.format("WHERE \n" + sqlNode.toString()));
        String expression = parseSqlNode(selectSQLBuilder, sqlNode).getValueForSubExpression();

        if (StringUtil.isNotEmpty(expression)) {
            if (expression.startsWith("(") && expression.endsWith(")")) {
                selectSQLBuilder.setExpression(expression.substring(1, expression.length() - 1));
            } else {
                selectSQLBuilder.setExpression("(" + expression + ")");
            }
            SelectSqlBuilder subSelectBudiler = selectSQLBuilder.getSubSelect();
            if (subSelectBudiler != null && subSelectBudiler.getWindowBuilder() != null && subSelectBudiler.getWindowBuilder().isShuffleOverWindow() && subSelectBudiler.getOverName() != null) {
                List<Expression> expressions = new ArrayList<>();
                ExpressionBuilder.createExpression("tmp", "tmp", expression, expressions, new ArrayList<>());
                if (expressions != null) {
                    for (Expression exp : expressions) {
                        if (exp.getVarName().equals(selectSQLBuilder.getSubSelect().getOverName())) {
                            int topN = Double.valueOf(exp.getValue().toString()).intValue();
                            subSelectBudiler.getWindowBuilder().setOverWindowTopN(topN);
                        }
                    }
                }
            }
        }
    }

    @Override
    public SelectSqlBuilder create(Properties configuration) {
        SelectSqlBuilder selectSqlBuilder = new SelectSqlBuilder();
        selectSqlBuilder.setConfiguration(configuration);
        return selectSqlBuilder;
    }
}
