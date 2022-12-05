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
package com.alibaba.rsqldb.parser.impl;

import com.alibaba.rsqldb.common.Constant;
import com.alibaba.rsqldb.parser.SqlParser;
import com.alibaba.rsqldb.parser.SqlParserBaseVisitor;
import com.alibaba.rsqldb.parser.model.Calculator;
import com.alibaba.rsqldb.parser.model.Columns;
import com.alibaba.rsqldb.parser.model.ListNode;
import com.alibaba.rsqldb.parser.model.Node;
import com.alibaba.rsqldb.parser.model.TableProperties;
import com.alibaba.rsqldb.parser.model.baseType.BooleanType;
import com.alibaba.rsqldb.parser.model.baseType.Literal;
import com.alibaba.rsqldb.parser.model.baseType.MultiLiteral;
import com.alibaba.rsqldb.parser.model.baseType.NumberType;
import com.alibaba.rsqldb.parser.model.baseType.StringType;
import com.alibaba.rsqldb.parser.model.statement.CreateViewStatement;
import com.alibaba.rsqldb.parser.model.expression.Expression;
import com.alibaba.rsqldb.parser.model.Field;
import com.alibaba.rsqldb.parser.model.FieldType;
import com.alibaba.rsqldb.parser.model.Function;
import com.alibaba.rsqldb.parser.model.statement.InsertQueryStatement;
import com.alibaba.rsqldb.parser.model.statement.InsertValueStatement;
import com.alibaba.rsqldb.parser.model.statement.query.phrase.JoinCondition;
import com.alibaba.rsqldb.parser.model.statement.query.phrase.JoinType;
import com.alibaba.rsqldb.parser.model.Operator;
import com.alibaba.rsqldb.parser.model.expression.SingleValueCalcuExpression;
import com.alibaba.rsqldb.parser.model.statement.query.GroupByQueryStatement;
import com.alibaba.rsqldb.parser.model.statement.query.QueryStatement;
import com.alibaba.rsqldb.parser.model.statement.query.SelectFieldResult;
import com.alibaba.rsqldb.parser.model.statement.query.SelectFunctionResult;
import com.alibaba.rsqldb.parser.model.statement.query.SelectType;
import com.alibaba.rsqldb.parser.model.statement.query.SelectTypeUtil;
import com.alibaba.rsqldb.parser.model.statement.query.SelectWindowResult;
import com.alibaba.rsqldb.parser.model.statement.CreateTableStatement;
import com.alibaba.rsqldb.parser.model.ColumnValue;
import com.alibaba.rsqldb.parser.model.statement.query.WindowInfo;
import com.alibaba.rsqldb.parser.model.expression.AndExpression;
import com.alibaba.rsqldb.parser.model.expression.MultiValueExpression;
import com.alibaba.rsqldb.parser.model.expression.OrExpression;
import com.alibaba.rsqldb.parser.model.expression.RangeValueExpression;
import com.alibaba.rsqldb.parser.model.expression.SingleValueExpression;
import com.alibaba.rsqldb.parser.model.statement.query.FilterQueryStatement;
import com.alibaba.rsqldb.parser.model.statement.query.WindowQueryStatement;
import com.alibaba.rsqldb.parser.model.statement.query.join.JointGroupByHavingStatement;
import com.alibaba.rsqldb.parser.model.statement.query.join.JointGroupByStatement;
import com.alibaba.rsqldb.parser.model.statement.query.join.JointStatement;
import com.alibaba.rsqldb.parser.model.statement.query.join.JointWhereGBHavingStatement;
import com.alibaba.rsqldb.parser.model.statement.query.join.JointWhereGroupByStatement;
import com.alibaba.rsqldb.parser.model.statement.query.join.JointWhereStatement;
import com.alibaba.rsqldb.parser.model.statement.query.phrase.GroupByPhrase;
import com.alibaba.rsqldb.parser.model.statement.query.phrase.HavingPhrase;
import com.alibaba.rsqldb.parser.model.statement.query.phrase.JoinPhrase;
import com.alibaba.rsqldb.parser.model.statement.query.phrase.WherePhrase;
import com.alibaba.rsqldb.parser.util.Pair;
import com.alibaba.rsqldb.parser.util.ParserUtil;
import org.antlr.v4.runtime.ParserRuleContext;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@SuppressWarnings("unchecked")
public class DefaultVisitor extends SqlParserBaseVisitor<Node> {
    @Override
    public Node visitSqlStatements(SqlParser.SqlStatementsContext ctx) {
        List<SqlParser.SqlStatementContext> sqlStatementContexts = ctx.sqlStatement();

        List<Node> result = sqlStatementContexts.stream().map(this::visit).collect(Collectors.toList());

        System.out.println("all over");

        return new ListNode<>(ctx, result);
    }

    @Override
    public Node visitSqlStatement(SqlParser.SqlStatementContext ctx) {
        return visit(ctx.sqlBody());
    }

    @Override
    public Node visitQueryStatement(SqlParser.QueryStatementContext ctx) {
        return visit(ctx.query());
    }

    @Override
    public Node visitQuery(SqlParser.QueryContext ctx) {
        SelectType selectType = SelectTypeUtil.whichType(ctx);

        SqlParser.SelectFieldContext selectFieldContext = ctx.selectField();
        ListNode<SelectFieldResult> selectFieldResults = (ListNode<SelectFieldResult>) visit(selectFieldContext);

        Map<Field, SelectFieldResult> selectFieldResultMap = new HashMap<>();
        Map<Field, Calculator> groupByCalculator = new HashMap<>();

        for (SelectFieldResult selectFieldResult : selectFieldResults) {
            Field field = selectFieldResult.getField();
            selectFieldResultMap.put(field, selectFieldResult);
            if (selectFieldResult instanceof SelectFunctionResult) {
                groupByCalculator.put(field, ((SelectFunctionResult) selectFieldResult).getCalculator());
            }
        }
        Set<Field> selectFields = selectFieldResultMap.keySet();

        String tableName = ParserUtil.getText(ctx.tableName());

        if (selectType == SelectType.SELECT_FROM) {
            return new QueryStatement(ctx, tableName, selectFields);
        }

        String asSourceTableName = null;
        SqlParser.IdentifierContext identifier = ctx.identifier();
        if (identifier != null) {
            StringType temp = (StringType) visit(identifier);
            asSourceTableName = temp.getResult();
        }

        JoinPhrase joinPhrase = (JoinPhrase) visit(ctx.joinPhrase());
        List<WherePhrase> wherePhrases = this.visit(ctx.wherePhrase(), WherePhrase.class);
        List<GroupByPhrase> groupByPhrases = this.visit(ctx.groupByPhrase(), GroupByPhrase.class);
        List<HavingPhrase> havingPhrases = this.visit(ctx.havingPhrase(), HavingPhrase.class);


        switch (selectType) {
            case SELECT_FROM_WHERE:
                return new FilterQueryStatement(ctx, tableName, selectFields, wherePhrases.get(0).getWhereExpression());
            case SELECT_FROM_GROUPBY:
                return new GroupByQueryStatement(ctx, tableName, selectFields, groupByCalculator, groupByPhrases.get(0).getGroupByFields());
            case SELECT_FROM_WHERE_GROUPBY:
                return new GroupByQueryStatement(ctx, tableName, selectFields, groupByCalculator,
                        groupByPhrases.get(0).getGroupByFields(), wherePhrases.get(0).getWhereExpression());
            case SELECT_FROM_GROUPBY_HAVING:
                return new GroupByQueryStatement(ctx, tableName, selectFields, groupByCalculator,
                        groupByPhrases.get(0).getGroupByFields(), havingPhrases.get(0).getHavingExpression());
            case SELECT_FROM_WHERE_GROUPBY_HAVING:
                return new GroupByQueryStatement(ctx, tableName, selectFields, groupByCalculator,
                        groupByPhrases.get(0).getGroupByFields(), wherePhrases.get(0).getWhereExpression(), havingPhrases.get(0).getHavingExpression());
            case SELECT_FROM_GROUPBY_WINDOW:
                return new WindowQueryStatement(ctx, tableName, selectFields, groupByCalculator,
                        groupByPhrases.get(0).getGroupByFields(), groupByPhrases.get(0).getWindowInfo());
            case SELECT_FROM_WHERE_GROUPBY_WINDOW:
                return new WindowQueryStatement(ctx, tableName, selectFields, groupByCalculator,
                        groupByPhrases.get(0).getGroupByFields(), groupByPhrases.get(0).getWindowInfo(), wherePhrases.get(0).getWhereExpression());
            case SELECT_FROM_GROUPBY_WINDOW_HAVING:
                return new WindowQueryStatement(ctx, tableName, selectFields, groupByCalculator,
                        groupByPhrases.get(0).getGroupByFields(), groupByPhrases.get(0).getWindowInfo(), havingPhrases.get(0).getHavingExpression());
            case SELECT_FROM_WHERE_GROUPBY_WINDOW_HAVING:
                return new WindowQueryStatement(ctx, tableName, selectFields, groupByCalculator,
                        groupByPhrases.get(0).getGroupByFields(), groupByPhrases.get(0).getWindowInfo(), wherePhrases.get(0).getWhereExpression(), havingPhrases.get(0).getHavingExpression());
            case SELECT_FROM_JOIN:
                return new JointStatement(ctx, tableName, selectFields, joinPhrase.getJoinType(), asSourceTableName,
                        joinPhrase.getJoinTableName(), joinPhrase.getAsJoinTableName(), joinPhrase.getJoinCondition());
            case SELECT_FROM_WHERE_JOIN: {
                Expression whereExpression = wherePhrases.get(0).getWhereExpression();
                return new JointWhereStatement(ctx, tableName, selectFields, joinPhrase.getJoinType(), asSourceTableName,
                        joinPhrase.getJoinTableName(), joinPhrase.getAsJoinTableName(), joinPhrase.getJoinCondition(), whereExpression, true);
            }
            case SELECT_FROM_WHERE_JOIN_WHERE: {
                return new JointWhereStatement(ctx, tableName, selectFields, joinPhrase.getJoinType(), asSourceTableName,
                        joinPhrase.getJoinTableName(), joinPhrase.getAsJoinTableName(), joinPhrase.getJoinCondition(),
                        wherePhrases.get(0).getWhereExpression(), wherePhrases.get(1).getWhereExpression());
            }
            case SELECT_FROM_WHERE_JOIN_WHERE_GROUPBY: {
                return new JointWhereGroupByStatement(ctx, tableName, selectFields, joinPhrase.getJoinType(), asSourceTableName,
                        joinPhrase.getJoinTableName(), joinPhrase.getAsJoinTableName(), joinPhrase.getJoinCondition(),
                        wherePhrases.get(0).getWhereExpression(), wherePhrases.get(1).getWhereExpression(), groupByCalculator,
                        groupByPhrases.get(0).getGroupByFields());
            }
            case SELECT_FROM_WHERE_JOIN_WHERE_GROUPBY_HAVING: {
                return new JointWhereGBHavingStatement(ctx, tableName, selectFields, joinPhrase.getJoinType(), asSourceTableName,
                        joinPhrase.getJoinTableName(), joinPhrase.getAsJoinTableName(), joinPhrase.getJoinCondition(),
                        wherePhrases.get(0).getWhereExpression(), wherePhrases.get(1).getWhereExpression(), groupByCalculator,
                        groupByPhrases.get(0).getGroupByFields(), havingPhrases.get(0).getHavingExpression());
            }
            case SELECT_FROM_WHERE_JOIN_GROUPBY: {
                return new JointWhereGroupByStatement(ctx, tableName, selectFields, joinPhrase.getJoinType(), asSourceTableName,
                        joinPhrase.getJoinTableName(), joinPhrase.getAsJoinTableName(), joinPhrase.getJoinCondition(),
                        wherePhrases.get(0).getWhereExpression(), true, groupByCalculator,
                        groupByPhrases.get(0).getGroupByFields());
            }
            case SELECT_FROM_WHERE_JOIN_GROUPBY_HAVING: {
                return new JointWhereGBHavingStatement(ctx, tableName, selectFields, joinPhrase.getJoinType(), asSourceTableName,
                        joinPhrase.getJoinTableName(), joinPhrase.getAsJoinTableName(), joinPhrase.getJoinCondition(),
                        wherePhrases.get(0).getWhereExpression(), true, groupByCalculator,
                        groupByPhrases.get(0).getGroupByFields(), havingPhrases.get(0).getHavingExpression());
            }
            case SELECT_FROM_JOIN_WHERE: {
                Expression whereExpression = wherePhrases.get(0).getWhereExpression();
                return new JointWhereStatement(ctx, tableName, selectFields, joinPhrase.getJoinType(), asSourceTableName,
                        joinPhrase.getJoinTableName(), joinPhrase.getAsJoinTableName(), joinPhrase.getJoinCondition(), whereExpression, false);
            }
            case SELECT_FROM_JOIN_WHERE_GROUPBY: {
                return new JointWhereGroupByStatement(ctx, tableName, selectFields, joinPhrase.getJoinType(), asSourceTableName,
                        joinPhrase.getJoinTableName(), joinPhrase.getAsJoinTableName(), joinPhrase.getJoinCondition(),
                        wherePhrases.get(0).getWhereExpression(), false, groupByCalculator,
                        groupByPhrases.get(0).getGroupByFields());
            }
            case SELECT_FROM_JOIN_WHERE_GROUPBY_HAVING: {
                return new JointWhereGBHavingStatement(ctx, tableName, selectFields, joinPhrase.getJoinType(), asSourceTableName,
                        joinPhrase.getJoinTableName(), joinPhrase.getAsJoinTableName(), joinPhrase.getJoinCondition(),
                        wherePhrases.get(0).getWhereExpression(), false, groupByCalculator,
                        groupByPhrases.get(0).getGroupByFields(), havingPhrases.get(0).getHavingExpression());
            }
            case SELECT_FROM_JOIN_GROUPBY: {
                return new JointGroupByStatement(ctx, tableName, selectFields, joinPhrase.getJoinType(), asSourceTableName,
                        joinPhrase.getJoinTableName(), joinPhrase.getAsJoinTableName(), joinPhrase.getJoinCondition(),
                        groupByCalculator, groupByPhrases.get(0).getGroupByFields());
            }
            case SELECT_FROM_JOIN_GROUPBY_HAVING: {
                return new JointGroupByHavingStatement(ctx, tableName, selectFields, joinPhrase.getJoinType(), asSourceTableName,
                        joinPhrase.getJoinTableName(), joinPhrase.getAsJoinTableName(), joinPhrase.getJoinCondition(),
                        groupByCalculator, groupByPhrases.get(0).getGroupByFields(), havingPhrases.get(0).getHavingExpression());
            }
            default: {
                throw new UnsupportedOperationException(ParserUtil.getText(ctx));
            }

        }
    }


    @Override
    public Node visitWherePhrase(SqlParser.WherePhraseContext ctx) {
        SqlParser.BooleanExpressionContext firstBooleanExpressionContext = ctx.booleanExpression();
        Expression whereExpression = (Expression) visit(firstBooleanExpressionContext);
        return new WherePhrase(ctx, whereExpression);
    }

    @Override
    public Node visitGroupByPhrase(SqlParser.GroupByPhraseContext ctx) {

        List<SqlParser.FieldNameContext> fieldNameContexts = ctx.fieldName();
        List<Field> groupByFields = fieldNameContexts.stream().map(this::visit).map(target -> (Field) target).collect(Collectors.toList());

        SqlParser.WindowFunctionContext windowFunctionContext = ctx.windowFunction();
        WindowInfo windowInfo = null;
        if (windowFunctionContext != null) {
            windowInfo = (WindowInfo) visit(windowFunctionContext);
        }

        return new GroupByPhrase(ctx, groupByFields, windowInfo);
    }

    @Override
    public Node visitHavingPhrase(SqlParser.HavingPhraseContext ctx) {
        SqlParser.BooleanExpressionContext secondBooleanExpressionContext = ctx.booleanExpression();

        Expression havingExpression = (Expression) visit(secondBooleanExpressionContext);

        return new HavingPhrase(ctx, havingExpression);
    }

    @Override
    public Node visitJoinPhrase(SqlParser.JoinPhraseContext ctx) {
        JoinType joinType;
        String leftJoin = ctx.LEFT().getText();
        if (!StringUtils.isEmpty(leftJoin)) {
            joinType = JoinType.LEFT_JOIN;
        } else {
            joinType = JoinType.INNER_JOIN;
        }

        String joinTableName = ParserUtil.getText(ctx.tableName());

        Object tableNameAsJoin = visit(ctx.identifier());
        String asJoinTableName = null;
        if (tableNameAsJoin != null) {
            StringType temp = (StringType) tableNameAsJoin;
            asJoinTableName = temp.getResult();
        }

        SqlParser.JoinConditionContext joinConditionContext = ctx.joinCondition();
        JoinCondition joinCondition = (JoinCondition) visit(joinConditionContext);

        return new JoinPhrase(ctx, joinType, joinTableName, asJoinTableName, joinCondition);
    }

    @Override
    public Node visitSelectField(SqlParser.SelectFieldContext ctx) {
        List<SelectFieldResult> result = new ArrayList<>();

        List<SqlParser.AsFieldContext> asFieldContexts = ctx.asField();

        if (asFieldContexts != null) {

            List<WindowInfo> windowInfos = new ArrayList<>();

            List<Object> collect = asFieldContexts.stream().map(this::visit).collect(Collectors.toList());
            for (Object item : collect) {
                if (item instanceof Field) {
                    Field field = (Field) item;
                    result.add(new SelectFieldResult(field));
                }

                if (item instanceof Function) {
                    Function function = (Function) item;
                    result.add(new SelectFunctionResult(function.getField(), function.getCalculator()));
                }

                if (item instanceof WindowInfo) {
                    WindowInfo windowInfo = (WindowInfo) item;
                    windowInfos.add(windowInfo);
                }
            }

            int index = 0;
            for (int i = 0; i < windowInfos.size(); i++) {
                WindowInfo windowInfo = windowInfos.get(i);
                index += windowInfo.getTargetTime().getIndex();
                if (i >= 2) {
                    throw new RuntimeException("window格式错误, select字段中包含一组window_start，window_end即可。");
                }
            }

            if (index != 0) {
                throw new RuntimeException("window格式错误, select字段中需要同时包含window_start，window_end。");
            }

            String windowStartFieldName = null;
            String windowEndFieldName = null;
            Field timestampField = null;
            for (WindowInfo windowInfo : windowInfos) {
                if (timestampField == null) {
                    timestampField = windowInfo.getTimeField();
                } else if (timestampField.equals(windowInfo.getTimeField())) {
                    throw new RuntimeException("window_start，window_end时间戳必须来自同一字段");
                }
                if (windowInfo.getTargetTime() == WindowInfo.TargetTime.WINDOW_START) {
                    windowStartFieldName = windowInfo.getNewFieldName();
                } else if (windowInfo.getTargetTime() == WindowInfo.TargetTime.WINDOW_END) {
                    windowEndFieldName = windowInfo.getNewFieldName();
                }

                SelectWindowResult selectWindowResult = new SelectWindowResult(timestampField, windowStartFieldName, windowEndFieldName);
                //用于校验
                selectWindowResult.setWindowInfo(windowInfo);
                result.add(selectWindowResult);
            }

        } else {
            String starText = ctx.STAR().getText();
            //默认是null，需要选择原表中所有字段；
        }

        return new ListNode<SelectFieldResult>(ctx, result);
    }

    @Override
    public Node visitJoinCondition(SqlParser.JoinConditionContext ctx) {
        List<JoinCondition> visit = this.visit(ctx.oneJoinCondition(), JoinCondition.class);

        JoinCondition condition = new JoinCondition(ctx);
        for (JoinCondition joinCondition : visit) {
            condition.addJoinCondition(joinCondition);
        }

        return condition;
    }

    @Override
    public Node visitOneJoinCondition(SqlParser.OneJoinConditionContext ctx) {
        SqlParser.FieldNameContext firstFieldNameContext = ctx.fieldName().get(0);
        SqlParser.FieldNameContext secondFieldNameContext = ctx.fieldName().get(1);

        Field firstField = (Field) visit(firstFieldNameContext);
        Field secondField = (Field) visit(secondFieldNameContext);

        assert ctx.EQUAL_SYMBOL().getSymbol().getType() == SqlParser.EQUAL_SYMBOL;

        JoinCondition condition = new JoinCondition(ctx);
        condition.addField(firstField, secondField);

        return condition;
    }

    @Override
    public Node visitCreateTable(SqlParser.CreateTableContext ctx) {
        SqlParser.IdentifierContext identifier = ctx.tableName().identifier();
        StringType tableName = (StringType) visit(identifier);

        SqlParser.TableDescriptorContext tableDescriptor = ctx.tableDescriptor();
        Columns columns = (Columns) visit(tableDescriptor);


        SqlParser.TablePropertiesContext propertiesContext = ctx.tableProperties();
        TableProperties properties = (TableProperties) visit(propertiesContext);

        CreateTableStatement result = new CreateTableStatement();

        result.setTableName(tableName.getResult());
        result.setColumns(columns);
        result.setProperties(properties.getHolder());

        return result;
    }

    @Override
    public Node visitCreateView(SqlParser.CreateViewContext ctx) {
        SqlParser.ViewNameContext viewNameContext = ctx.viewName();
        StringType viewTableName = (StringType) visit(viewNameContext.identifier());
        QueryStatement queryStatement = (QueryStatement) visit(ctx.query());
        return new CreateViewStatement(ctx, viewTableName.getResult(), queryStatement);
    }

    @Override
    public Node visitInsertValue(SqlParser.InsertValueContext ctx) {
        String tableName = ctx.tableName().getText();

        SqlParser.TableDescriptorContext tableDescriptor = ctx.tableDescriptor();
        Columns targetColumns = null;
        if (tableDescriptor != null) {
            targetColumns = (Columns) visit(tableDescriptor);
        }

        SqlParser.ValuesContext valuesContext = ctx.values();
        List<String> values = (List<String>) visit(valuesContext);

        if (targetColumns != null && targetColumns.getHolder().size() != values.size()) {
            throw new IllegalArgumentException("number of value is not correct.");
        }




        List<ColumnValue> list = new ArrayList<>();
        if (targetColumns != null) {
            for (int i = 0; i < targetColumns.getHolder().size(); i++) {
                Pair<String, FieldType> pair = targetColumns.getHolder().get(i);

                ColumnValue columnValue = new ColumnValue();
                columnValue.setFieldName(pair.getKey());
                columnValue.setFieldType(pair.getValue());
                columnValue.setValue(values.get(i));

                list.add(columnValue);
            }
        } else {
            for (String value : values) {
                ColumnValue temp = new ColumnValue();
                temp.setValue(value);

                list.add(temp);
            }
        }

        return new InsertValueStatement(ctx, tableName, list);
    }

    @Override
    public Node visitInsertSelect(SqlParser.InsertSelectContext ctx) {
        String tableName = ctx.tableName().getText();

        SqlParser.TableDescriptorContext tableDescriptor = ctx.tableDescriptor();
        Columns targetColumns = null;
        if (tableDescriptor != null) {
            targetColumns = (Columns) visit(tableDescriptor);
        }

        QueryStatement queryStatement = (QueryStatement) visit(ctx.query());

        if (targetColumns == null) {
            return new InsertQueryStatement(ctx, tableName, queryStatement);
        } else {
            return new InsertQueryStatement(ctx, tableName, queryStatement, targetColumns);
        }
    }

    @Override
    public Node visitTableDescriptor(SqlParser.TableDescriptorContext ctx) {
        List<SqlParser.ColumnDescriptorContext> list = ctx.columnDescriptor();
        List<Columns> temp = this.visit(list, Columns.class);

        Columns columns = new Columns(ctx);

        for (Columns item : temp) {
            columns.addColumns(item.getHolder());
        }

        return columns;
    }

    @Override
    public Node visitColumnDescriptor(SqlParser.ColumnDescriptorContext ctx) {
        System.out.println("visitColumnDescriptor");

        Columns columns = new Columns(ctx);

        SqlParser.IdentifierContext identifierContext = ctx.identifier();
        StringType columnName = (StringType) visit(identifierContext);

        FieldType type = null;
        SqlParser.DataTypeContext dataTypeContext = ctx.dataType();
        if (dataTypeContext != null) {
            StringType columnType = (StringType) visit(dataTypeContext);

            type = FieldType.getByType(columnType.getResult());
        }

        columns.addFieldNameAndType(columnName.getResult(), type);

        return columns;
    }

    @Override
    public Node visitTableProperties(SqlParser.TablePropertiesContext ctx) {
        List<SqlParser.TablePropertyContext> tablePropertyContexts = ctx.tableProperty();
        List<TableProperties> temp = this.visit(tablePropertyContexts, TableProperties.class);

        TableProperties result = new TableProperties();
        for (TableProperties tableProperties : temp) {
            result.addProperties(tableProperties.getHolder());
        }

        return result;
    }

    @Override
    public Node visitTableProperty(SqlParser.TablePropertyContext ctx) {
        System.out.println("visitTableProperty");

        SqlParser.IdentifierContext identifier = ctx.identifier();

        StringType temp = (StringType) visit(identifier);
        String key = temp.getResult();


        SqlParser.LiteralContext literal = ctx.literal();
        Literal<?> result = (Literal<?>) visit(literal);

        TableProperties tableProperties = new TableProperties();
        tableProperties.addProperties(key, result);

        return tableProperties;
    }


    @Override
    public Node visitAsFieldName(SqlParser.AsFieldNameContext ctx) {
        Field field = (Field) visit(ctx.fieldName());

        String newFieldName = getIdentifier(ctx.identifier());

        field.setAsFieldName(newFieldName);
        return field;
    }

    @Override
    public Node visitAsFunctionField(SqlParser.AsFunctionFieldContext ctx) {
        Function function = (Function) visit(ctx.function());
        String newFieldName = getIdentifier(ctx.identifier());

        function.getField().setAsFieldName(newFieldName);

        return function;
    }

    @Override
    public Node visitAsWindowFunctionField(SqlParser.AsWindowFunctionFieldContext ctx) {
        WindowInfo windowInfo = (WindowInfo) visit(ctx.windowFunction());

        String newFieldName = getIdentifier(ctx.identifier());

        windowInfo.setNewFieldName(newFieldName);

        return windowInfo;
    }

    @Override
    public Node visitFunction(SqlParser.FunctionContext ctx) {
        String text = ctx.calculator().getText();
        Calculator calculator = Calculator.valueOf(text);

        SqlParser.FieldNameContext fieldNameContext = ctx.fieldName();
        if (fieldNameContext != null) {
            Field field = (Field) visit(fieldNameContext);
            return new Function(ctx, calculator, field);
        }

        String star = ctx.STAR().getText();
        if (!StringUtils.isEmpty(star)) {
            Field field = new Field(ctx, Constant.STAR);
            return new Function(ctx, calculator, field);
        }

        throw new IllegalArgumentException("parser function error: " + ParserUtil.getText(ctx));
    }

    //一个sql会返回三个windowInfo
    @Override
    public Node visitTumbleWindow(SqlParser.TumbleWindowContext ctx) {
        SqlParser.Tumble_windowContext tumbleWindowContext = ctx.tumble_window();

        String tumble = tumbleWindowContext.TUMBLE().getText();
        String tumbleStart = tumbleWindowContext.TUMBLE_START().getText();
        String tumbleEnd = tumbleWindowContext.TUMBLE_END().getText();

        Field field = (Field) visit(tumbleWindowContext.fieldName());
        NumberType time = (NumberType) visit(tumbleWindowContext.QUOTED_NUMBER());
        long size = (Long) time.getResult();

        TimeUnit timeUnit = ParserUtil.getTimeUnit(tumbleWindowContext.timeunit().getText());

        assert timeUnit != null;
        long secondSize = timeUnit.toSeconds(size);

        WindowInfo windowInfo = new WindowInfo(ctx, WindowInfo.WindowType.TUMBLE, secondSize, secondSize, field);
        if (!StringUtils.isEmpty(tumbleStart)) {
            windowInfo.setTargetTime(WindowInfo.TargetTime.WINDOW_START);
        } else if (!StringUtils.isEmpty(tumbleEnd)) {
            windowInfo.setTargetTime(WindowInfo.TargetTime.WINDOW_END);
        }

        return windowInfo;
    }

    @Override
    public Node visitHopWindow(SqlParser.HopWindowContext ctx) {
        SqlParser.Hop_windowContext hopWindowContext = ctx.hop_window();

        String text = hopWindowContext.HOP().getText();
        String hopStart = hopWindowContext.HOP_START().getText();
        String hopEnd = hopWindowContext.HOP_END().getText();

        Field field = (Field) visit(hopWindowContext.fieldName());


        NumberType slideSize = (NumberType) visit(hopWindowContext.QUOTED_NUMBER(0));
        TimeUnit slideTimeUnit = ParserUtil.getTimeUnit(hopWindowContext.timeunit(0).getText());
        long slide = (Long) slideSize.getResult();

        NumberType windowSize = (NumberType) visit(hopWindowContext.QUOTED_NUMBER(1));
        TimeUnit windowTimeUnit = ParserUtil.getTimeUnit(hopWindowContext.timeunit(1).getText());
        long size = (Long) windowSize.getResult();


        assert windowTimeUnit != null;
        long secondSize = windowTimeUnit.toSeconds(size);
        assert slideTimeUnit != null;
        long secondSlide = slideTimeUnit.toSeconds(slide);

        WindowInfo windowInfo = new WindowInfo(ctx, WindowInfo.WindowType.HOP, secondSlide, secondSize, field);
        if (!StringUtils.isEmpty(hopStart)) {
            windowInfo.setTargetTime(WindowInfo.TargetTime.WINDOW_START);
        } else if (!StringUtils.isEmpty(hopEnd)) {
            windowInfo.setTargetTime(WindowInfo.TargetTime.WINDOW_END);
        }

        return windowInfo;
    }

    @Override
    public Node visitSessionWindow(SqlParser.SessionWindowContext ctx) {
        SqlParser.Session_windowContext sessionWindowContext = ctx.session_window();

        String session = sessionWindowContext.SESSION().getText();
        String sessionStart = sessionWindowContext.SESSION_START().getText();
        String sessionEnd = sessionWindowContext.SESSION_END().getText();

        Field field = (Field) visit(sessionWindowContext.fieldName());
        NumberType num = (NumberType) visit(sessionWindowContext.QUOTED_NUMBER());
        TimeUnit timeUnit = ParserUtil.getTimeUnit(sessionWindowContext.timeunit().getText());
        //todo 异常体系
        long size = (Long) num.getResult();

        assert timeUnit != null;
        long secondSize = timeUnit.toSeconds(size);

        WindowInfo windowInfo = new WindowInfo(ctx, WindowInfo.WindowType.SESSION, secondSize, secondSize, field);
        if (!StringUtils.isEmpty(sessionStart)) {
            windowInfo.setTargetTime(WindowInfo.TargetTime.WINDOW_START);
        } else if (!StringUtils.isEmpty(sessionEnd)) {
            windowInfo.setTargetTime(WindowInfo.TargetTime.WINDOW_END);
        }

        return windowInfo;
    }

    @Override
    public Node visitJointExpression(SqlParser.JointExpressionContext ctx) {
        SqlParser.BooleanExpressionContext leftExpressionContext = ctx.booleanExpression(0);
        SqlParser.BooleanExpressionContext rightExpressionContext = ctx.booleanExpression(1);


        Expression left = (Expression) visit(leftExpressionContext);
        Expression right = (Expression) visit(rightExpressionContext);

        if (ctx.AND().getSymbol().getType() == SqlParser.AND) {
            return new AndExpression(ctx, left, right);
        }

        if (ctx.OR().getSymbol().getType() == SqlParser.OR) {
            return new OrExpression(ctx, left, right);
        }


        throw new IllegalArgumentException("unrecognizable sql: " + ParserUtil.getText(ctx));
    }

    @Override
    public Node visitOperatorExpression(SqlParser.OperatorExpressionContext ctx) {
        Field field = (Field) visit(ctx.fieldName());

        String operatorText = ctx.operator().getText();
        Operator operator = ParserUtil.getOperator(operatorText);

        Node node = visit(ctx.literal());
        if (node == null) {
            return new SingleValueExpression(ctx, field, operator, null);
        } else {
            return new SingleValueExpression(ctx, field, operator, (Literal<?>) node);
        }

    }

    @Override
    public Node visitIsNullExpression(SqlParser.IsNullExpressionContext ctx) {
        Field field = (Field) visit(ctx.fieldName());

        assert ctx.IS().getSymbol().getType() == SqlParser.IS;
        assert ctx.NULL().getSymbol().getType() == SqlParser.NULL;

        return new SingleValueExpression(ctx, field, Operator.EQUAL, null);
    }

    @Override
    public Node visitBetweenExpression(SqlParser.BetweenExpressionContext ctx) {
        Field field = (Field) visit(ctx.fieldName());

        String low = ctx.NUMBER(0).getText();
        String high = ctx.NUMBER(1).getText();


        return new RangeValueExpression(ctx, field, Operator.BETWEEN_AND, Long.parseLong(low), Long.parseLong(high));
    }

    @Override
    public Node visitInExpression(SqlParser.InExpressionContext ctx) {
        Field field = (Field) visit(ctx.fieldName());
        assert ctx.IN().getSymbol().getType() == SqlParser.IN;
        MultiLiteral visit = (MultiLiteral) visit(ctx.values());

        return new MultiValueExpression(ctx, field, Operator.IN, visit);
    }

    @Override
    public Node visitFunctionExpression(SqlParser.FunctionExpressionContext ctx) {
        Function function = (Function) visit(ctx.function());

        String operatorText = ctx.operator().getText();
        Operator operator = ParserUtil.getOperator(operatorText);

        Node node = visit(ctx.literal());
        if (node == null) {
            return new SingleValueCalcuExpression(ctx, function.getField(), operator, null, function.getCalculator());
        } else {
            Literal<?> literal = (Literal<?>) node;
            return new SingleValueCalcuExpression(ctx, function.getField(), operator, literal, function.getCalculator());
        }
    }

    @Override
    public Node visitFieldName(SqlParser.FieldNameContext ctx) {
        String tableName = null;
        String fieldName = null;

        Object identifier = visit(ctx.identifier());
        if (identifier != null) {
            StringType fieldNameType = (StringType) identifier;
            fieldName = fieldNameType.getLiteral();
        } else {
            StringType tableNameType = (StringType) visit(ctx.tableName());
            StringType fieldNameType = (StringType) visit(ctx.identifier());

            tableName = tableNameType.getLiteral();
            fieldName = fieldNameType.getLiteral();
        }
        return new Field(ctx, tableName, fieldName);
    }


    @Override
    public Node visitAlphabetIdentifier(SqlParser.AlphabetIdentifierContext ctx) {
        String text = ctx.ALPHABET_STRING().getText();
        return new StringType(ctx, text);
    }

    @Override
    public Node visitQuotedIdentifier(SqlParser.QuotedIdentifierContext ctx) {
        String text = ctx.QUOTED_STRING().getText();

        if (text == null) {
            return null;
        }

        text = text.substring(1, text.length() - 1);

        return new StringType(ctx, text);
    }

    @Override
    public Node visitBackQuotedIdentifier(SqlParser.BackQuotedIdentifierContext ctx) {
        String text = ctx.BACKQUOTED_STRING().getText();
        if (text == null) {
            return null;
        }

        text = text.substring(1, text.length() - 1);
        return new StringType(ctx, text);
    }

    @Override
    public Node visitNumIdentifier(SqlParser.NumIdentifierContext ctx) {
        String text = ctx.NUM_STRING().getText();

        if (text == null) {
            return null;
        }

        text = text.substring(1, text.length() - 1);
        return new StringType(ctx, text);
    }

    @Override
    public Node visitStringIdentifier(SqlParser.StringIdentifierContext ctx) {
        String text = ctx.STRING().getText();

        if (text == null) {
            return null;
        }

        text = text.substring(1, text.length() - 1);
        return new StringType(ctx, text);
    }

    @Override
    public Node visitVariable(SqlParser.VariableContext ctx) {
        throw new UnsupportedOperationException(ParserUtil.getText(ctx));
    }

    @Override
    public Node visitValues(SqlParser.ValuesContext ctx) {
        List<SqlParser.LiteralContext> literals = ctx.literal();
        List<Literal<?>> values = literals.stream().map(this::visit).map(value -> (Literal<?>) value).collect(Collectors.toList());
        return new MultiLiteral(ctx, values);
    }

    @Override
    public Node visitNullLiteral(SqlParser.NullLiteralContext ctx) {
        return new StringType(ctx, null);
    }

    @Override
    public Node visitBooleanLiteral(SqlParser.BooleanLiteralContext ctx) {
        String falseBoolean = ctx.FALSE().getText();
        String trueBoolean = ctx.TRUE().getText();

        boolean value;
        if (falseBoolean != null) {
            value = Boolean.parseBoolean(falseBoolean);
        } else {
            value = Boolean.parseBoolean(trueBoolean);
        }

        return new BooleanType(ctx, value);
    }

    @Override
    public Node visitNumberLiteral(SqlParser.NumberLiteralContext ctx) {
        String text = ctx.NUMBER().getText();
        Number result = 0;
        if (text.contains(".")) {
            result = Double.valueOf(text);
        } else {
            result = Long.valueOf(text);
        }
        return new NumberType(ctx, result);
    }

    @Override
    public Node visitStringLiteral(SqlParser.StringLiteralContext ctx) {
        String text = ctx.STRING().getText();

        if (text == null) {
            return null;
        }

        text = text.substring(1, text.length() - 1);

        return new StringType(ctx, text);
    }

    @Override
    public Node visitVariableLiteral(SqlParser.VariableLiteralContext ctx) {
        throw new UnsupportedOperationException(ParserUtil.getText(ctx));
    }

    @Override
    public Node visitQuotedNumberLiteral(SqlParser.QuotedNumberLiteralContext ctx) {
        String text = ctx.QUOTED_NUMBER().getText();
        if (text == null) {
            return null;
        }

        text = text.substring(1, text.length() - 1);

        Number result = 0;
        if (text.contains(".")) {
            result = Double.valueOf(text);
        } else {
            result = Long.valueOf(text);
        }
        return new NumberType(ctx, result);
    }

    @Override
    public Node visitQuotedStringLiteral(SqlParser.QuotedStringLiteralContext ctx) {
        String text = ctx.QUOTED_STRING().getText();
        if (text == null) {
            return null;
        }

        text = text.substring(1, text.length() - 1);

        return new StringType(ctx, text);
    }

    private String getIdentifier(SqlParser.IdentifierContext identifier) {
        if (identifier != null) {
            Literal<String> literal = (Literal<String>) visit(identifier);
            return literal.getResult();
        }
        return null;
    }

    private <T> List<T> visit(List<? extends ParserRuleContext> context, Class<T> clazz) {
        if (context == null || context.size() == 0) {
            return new ArrayList<>();
        }

        return context.stream().map(this::visit).map(clazz::cast).collect(Collectors.toList());
    }
}
