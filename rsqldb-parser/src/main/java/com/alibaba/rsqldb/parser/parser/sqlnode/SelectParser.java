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
package com.alibaba.rsqldb.parser.parser.sqlnode;

import com.alibaba.rsqldb.parser.parser.ISqlParser;
import com.alibaba.rsqldb.parser.parser.SQLNodeParserFactory;
import com.alibaba.rsqldb.parser.parser.builder.JoinSQLBuilder;
import com.alibaba.rsqldb.parser.parser.builder.SelectSQLBuilder;
import com.alibaba.rsqldb.parser.parser.builder.UnionSQLBuilder;
import com.alibaba.rsqldb.parser.parser.function.HopParser;
import com.alibaba.rsqldb.parser.parser.result.BuilderParseResult;
import com.alibaba.rsqldb.parser.parser.result.IParseResult;
import com.alibaba.rsqldb.parser.parser.result.ScriptParseResult;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.SQLFormatterUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.filter.builder.ExpressionBuilder;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;

public class SelectParser extends AbstractSelectNodeParser<SqlSelect> {

    private static final Log LOG = LogFactory.getLog(SelectParser.class);
    private SQLFormatterUtil sqlFormatterUtil=new SQLFormatterUtil();
    @Override
    public IParseResult parse(SelectSQLBuilder tableDescriptor, SqlSelect sqlSelect) {
        //
        tableDescriptor.setSqlNode(sqlSelect);
        //
        boolean isNeedWhereToCondition = parseFrom(tableDescriptor, sqlSelect);
        //
        parseWhere(tableDescriptor, sqlSelect, isNeedWhereToCondition);
        //
        parseGroup(tableDescriptor, sqlSelect);
        //
        //TODO parseHaving(); parseOn();
        //
        parseSelect(tableDescriptor, sqlSelect);
        //
        tableDescriptor.addDependentTable(getDependentTable(tableDescriptor));
        //
        return new BuilderParseResult(tableDescriptor);
    }

    protected void parseGroup(SelectSQLBuilder builder, SqlSelect sqlSelect) {
        if (sqlSelect.getGroup() == null) {
            return;
        }
        SqlNodeList groups = sqlSelect.getGroup();
        builder.setGroupSQL(sqlFormatterUtil.format("GROUP \n"+groups.toString()));
        List<SqlNode> list = groups.getList();
        for (SqlNode sqlNode : list) {
            IParseResult result = parseSqlNode(builder, sqlNode);
            if (result.getReturnValue() != null) {
                if (builder.getWindowBuilder() == null) {
                    HopParser.createWindowBuilder(builder);
                }
                builder.getWindowBuilder().getGroupByFieldNames().add(result.getReturnValue());
            }
        }
        if (sqlSelect.getHaving() != null) {
            boolean isSelectStage = builder.isSelectStage();
            boolean isFromStage = builder.isFromStage();
            builder.switchWhere();
            SqlNode sqlNode = sqlSelect.getHaving();
            builder.setHavingSQL(sqlFormatterUtil.format("HAVING \n"+sqlNode.toString()));
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
    private String getDependentTable(SelectSQLBuilder tableDescriptor) {
        String tableName = tableDescriptor.getTableName();
        SelectSQLBuilder current = tableDescriptor;
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
    protected void parseSelect(SelectSQLBuilder sqlBuilder, SqlSelect sqlSelect) {
        sqlBuilder.setDistinct(sqlSelect.isDistinct());
        List<String> scripts = sqlBuilder.getScripts();
        sqlBuilder.setScripts(new ArrayList<>());
        sqlBuilder.switchSelect();
        SqlNodeList sqlNodes = sqlSelect.getSelectList();
        sqlBuilder.setSelectSQL( sqlFormatterUtil.format("SELECT \n"+sqlNodes.toString()));


        for (SqlNode sqlNode : sqlNodes.getList()) {
            ISqlParser sqlParser = SQLNodeParserFactory.getParse(sqlNode);
            if (sqlParser == null) {
                throw new RuntimeException(sqlNode.toString() + " not have parser to parser,may be have error");
            }

            SelectSQLBuilder builder = sqlBuilder;
            if (sqlBuilder.getWindowBuilder() != null) {
                /**
                 * 期望每一个字段和它所有的脚本放到一起，而不是把所有字段的脚本混在一起
                 */
                SelectSQLBuilder selectSQLBuilder = new SelectSQLBuilder();
                selectSQLBuilder.setTableName(sqlBuilder.getTableName());
                selectSQLBuilder.setAsName(sqlBuilder.getAsName());
                selectSQLBuilder.setUnionSQLBuilder(sqlBuilder.getUnionSQLBuilder());
                selectSQLBuilder.setSubSelect(sqlBuilder.getSubSelect());
                selectSQLBuilder.setJoinSQLDescriptor(sqlBuilder.getJoinSQLDescriptor());
                builder = selectSQLBuilder;

            }
            IParseResult sqlVar = sqlParser.parse(builder, sqlNode);
            String varName = sqlVar.getReturnValue();
            if (StringUtil.isEmpty(varName)) {
                throw new RuntimeException("the field parser error " + sqlNode.toString());
            }
            if (sqlBuilder.getWindowBuilder() != null) {
                ScriptParseResult scriptParseResult = new ScriptParseResult();
                List<String> scriptList = builder.getScripts();
                if (scriptList != null) {
                    for (String script : scriptList) {
                        scriptParseResult.addScript(script);
                    }
                }
                if (StringUtil.isEmpty(varName)) {
                    throw new RuntimeException("the field parser error " + sqlNode.toString());
                }
                sqlBuilder.putSelectField(varName, scriptParseResult);
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

    private static final UnionParser unionParser = new UnionParser();

    /**
     * 如果是嵌套表，则解析嵌套部分，否则解析出表名
     *
     * @param rootSelectSQLBuilder
     * @param sqlSelect
     */
    protected boolean parseFrom(SelectSQLBuilder rootSelectSQLBuilder, SqlSelect sqlSelect) {
        rootSelectSQLBuilder.switchFrom();
        SqlNode from = sqlSelect.getFrom();
        if (from == null) {
            return false;
        }
        String aliasName = null;
        /**
         * 有 as的场景
         */
        if (from instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) from;
            if (sqlBasicCall.getOperator().getName().toLowerCase().equals("as")) {
                from = sqlBasicCall.getOperandList().get(0);
                aliasName = sqlBasicCall.getOperandList().get(1).toString();
            }
            rootSelectSQLBuilder.setFromSQL("FROM "+from+" as "+ aliasName);
        }

        /**
         * 是子查询的场景
         */
        if (from instanceof SqlSelect) {
            SelectSQLBuilder selectSQLDescriptor = new SelectSQLBuilder();
            selectSQLDescriptor.setParentSelect(rootSelectSQLBuilder);
            selectSQLDescriptor.setSqlNode(from);
            rootSelectSQLBuilder.setSubSelect(selectSQLDescriptor);
            if (StringUtil.isNotEmpty(aliasName)) {
                selectSQLDescriptor.setAsName(aliasName);
            }
            parse(selectSQLDescriptor, (SqlSelect) from);
            rootSelectSQLBuilder.setTableName(selectSQLDescriptor.getTableName());

            if(aliasName!=null){
                rootSelectSQLBuilder.setFromSQL("FROM "+aliasName);
            }else {
                rootSelectSQLBuilder.setFromSQL("FROM "+"sub_select_table");
            }
            /**
             * join的场景
             */
        } else if (from instanceof SqlJoin) {
            rootSelectSQLBuilder.setSupportOptimization(false);
            JoinSQLBuilder joinSQLBuilder = new JoinSQLBuilder();
            joinSQLBuilder.setTableName2Builders(rootSelectSQLBuilder.getTableName2Builders());
            IParseResult result = SQLNodeParserFactory.getParse(from).parse(joinSQLBuilder, from);
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
            rootSelectSQLBuilder.setFromSQL("FROM "+joinSQLBuilder.getLeft().getReturnValue()+ joinSQLBuilder.getJoinType()+" "+joinSQLBuilder.getRight().getReturnValue());
            return joinSQLBuilder.isNeedWhereToCondition();
            /**
             * union all场景
             */
        } else if (unionParser.support(from)) {
            UnionSQLBuilder unionSQLBuilder = unionParser.create();
            if (StringUtil.isNotEmpty(aliasName)) {
                rootSelectSQLBuilder.setAsName(aliasName);
            }
            unionParser.parse(unionSQLBuilder, (SqlBasicCall) from);
//            Set<String> tableNames = unionSQLBuilder.parseDependentTables();
////            if (tableNames == null || tableNames.size() > 1) {
////                throw new RuntimeException("can not support union parser, expect one source。");
////            }
////            String tableName = tableNames.iterator().next();
            rootSelectSQLBuilder.setTableName(unionSQLBuilder.getTableName());
            rootSelectSQLBuilder.setUnionSQLBuilder(unionSQLBuilder);

            rootSelectSQLBuilder.setFromSQL("FROM "+"union("+MapKeyUtil.createKeyFromCollection(",",unionSQLBuilder.getTableNames())+")");
        } else {//单表名的场景
            rootSelectSQLBuilder.setTableName(from.toString());
            if (StringUtil.isNotEmpty(aliasName)) {
                rootSelectSQLBuilder.setAsName(aliasName);
                rootSelectSQLBuilder.setFromSQL(from.toString()+" as "+aliasName );
            }else {
                rootSelectSQLBuilder.setFromSQL(from.toString());
            }
            rootSelectSQLBuilder.setFromSQL("FROM "+from.toString());

            //tableDescriptor.addDependentTable(tableDescriptor.getTableName());
        }
        return false;
    }

    /**
     * 主要是解析where部分，有两种解析，如果有函数，转换成脚本函数，如果是表达式，转换成规则引擎表达式
     *
     * @param selectSQLBuilder
     * @param sqlSelect
     */
    protected void parseWhere(SelectSQLBuilder selectSQLBuilder, SqlSelect sqlSelect, boolean isNeedWhereToCondition) {
        if (isNeedWhereToCondition) {
            SqlNode sqlNode = sqlSelect.getWhere();
            selectSQLBuilder.setJoinConditionSQL(sqlFormatterUtil.format("ON \n"+sqlNode.toString()));
            if (sqlNode == null) {
                throw new RuntimeException("expect join condition from next where, but where is null " + sqlSelect.toString());
            }
            ISqlParser sqlParser = SQLNodeParserFactory.getParse(sqlNode);
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
            JoinSQLBuilder joinSQLBuilder = selectSQLBuilder.getJoinSQLDescriptor();
            joinSQLBuilder.setOnCondition(result.getValueForSubExpression());
            joinSQLBuilder.setConditionSqlNode(sqlNode);
            return;
        }

        selectSQLBuilder.switchWhere();
        SqlNode sqlNode = sqlSelect.getWhere();
        if (sqlNode == null) {
            return;
        }
            selectSQLBuilder.setWhereSQL(sqlFormatterUtil.format("WHERE \n"+sqlNode.toString()));
        String expression = parseSqlNode(selectSQLBuilder, sqlNode).getValueForSubExpression();

        if (StringUtil.isNotEmpty(expression)) {
            if (expression.startsWith("(") && expression.endsWith(")")) {
                selectSQLBuilder.setExpression(expression.substring(1, expression.length() - 1));
            } else {
                selectSQLBuilder.setExpression("(" + expression + ")");
            }
            SelectSQLBuilder subSelectBudiler = selectSQLBuilder.getSubSelect();
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
    public SelectSQLBuilder create() {
        return new SelectSQLBuilder();
    }
}
