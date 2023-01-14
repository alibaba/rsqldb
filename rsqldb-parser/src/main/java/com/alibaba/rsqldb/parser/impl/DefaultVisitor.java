 /*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rsqldb.parser.impl;

import com.alibaba.rsqldb.common.RSQLConstant;
import com.alibaba.rsqldb.common.exception.RSQLServerException;
import com.alibaba.rsqldb.common.exception.SyntaxErrorException;
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
import com.alibaba.rsqldb.parser.model.statement.query.phrase.ExpressionType;
import com.alibaba.rsqldb.parser.model.statement.query.phrase.JoinCondition;
import com.alibaba.rsqldb.parser.model.statement.query.phrase.JoinType;
import com.alibaba.rsqldb.parser.model.Operator;
import com.alibaba.rsqldb.parser.model.expression.SingleValueCalcuExpression;
import com.alibaba.rsqldb.parser.model.statement.query.GroupByQueryStatement;
import com.alibaba.rsqldb.parser.model.statement.query.QueryStatement;
import com.alibaba.rsqldb.parser.model.statement.query.SelectFieldResult;
import com.alibaba.rsqldb.parser.model.statement.query.SelectFunctionResult;
import com.alibaba.rsqldb.parser.model.statement.SQLType;
import com.alibaba.rsqldb.parser.model.statement.query.SelectTypeUtil;
import com.alibaba.rsqldb.parser.model.statement.query.SelectWindowResult;
import com.alibaba.rsqldb.parser.model.statement.CreateTableStatement;
import com.alibaba.rsqldb.parser.model.ColumnValue;
import com.alibaba.rsqldb.parser.model.statement.query.WindowInfoInSQL;
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
import com.alibaba.rsqldb.parser.util.ParserUtil;
import com.alibaba.rsqldb.parser.util.Validator;
import org.antlr.v4.runtime.InputMismatchException;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.streams.core.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@SuppressWarnings("unchecked")
public class DefaultVisitor extends SqlParserBaseVisitor<Node> {
    private static final Logger logger = LoggerFactory.getLogger(DefaultVisitor.class);

    @Override
    public Node visitSqlStatements(SqlParser.SqlStatementsContext ctx) {
        List<SqlParser.SqlStatementContext> sqlStatementContexts = ctx.sqlStatement();

        List<Node> result = sqlStatementContexts.stream().map(this::visit).collect(Collectors.toList());

        return new ListNode<>(ParserUtil.getText(ctx), result);
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
        SQLType SQLType = SelectTypeUtil.whichType(ctx);

        SqlParser.SelectFieldContext selectFieldContext = ctx.selectField();
        ListNode<SelectFieldResult> selectFieldResults = (ListNode<SelectFieldResult>) visit(selectFieldContext);

        Map<Field, Calculator> selectFieldAndCalculator = new HashMap<>();
        List<WindowInfoInSQL> windowInfoInSQLS = new ArrayList<>();

        //将select分类
        for (SelectFieldResult selectFieldResult : selectFieldResults) {
            Field field = selectFieldResult.getField();
            selectFieldAndCalculator.put(field, null);
            if (selectFieldResult instanceof SelectFunctionResult) {
                selectFieldAndCalculator.put(field, ((SelectFunctionResult) selectFieldResult).getCalculator());
            }
            if (selectFieldResult instanceof SelectWindowResult) {
                SelectWindowResult windowResult = (SelectWindowResult) selectFieldResult;
                windowInfoInSQLS.add(windowResult.getWindowInfo());

                selectFieldAndCalculator.put(field, windowResult.getCalculator());
            }
        }

        String tableName = ParserUtil.getText(ctx.tableName());

        String asSourceTableName = null;
        SqlParser.IdentifierContext identifier = ctx.identifier();
        if (identifier != null) {
            StringType temp = (StringType) visit(identifier);
            asSourceTableName = temp.result();
        }

        JoinPhrase joinPhrase = null;
        if (ctx.joinPhrase() != null) {
            joinPhrase = (JoinPhrase) visit(ctx.joinPhrase());
        }
        List<WherePhrase> wherePhrases = this.visit(ctx.wherePhrase(), WherePhrase.class);
        List<GroupByPhrase> groupByPhrases = this.visit(ctx.groupByPhrase(), GroupByPhrase.class);
        for (GroupByPhrase groupByPhrase : groupByPhrases) {
            if (groupByPhrase.getWindowInfo() != null) {
                windowInfoInSQLS.add(groupByPhrase.getWindowInfo());
            }
        }
        List<HavingPhrase> havingPhrases = this.visit(ctx.havingPhrase(), HavingPhrase.class);

        //校验sql中各处window信息是否一样
        Validator.window(windowInfoInSQLS);
        String content = ParserUtil.getText(ctx);
        switch (SQLType) {
            case SELECT_FROM:
                return new QueryStatement(content, tableName, selectFieldAndCalculator);
            case SELECT_FROM_WHERE:
                return new FilterQueryStatement(content, tableName, selectFieldAndCalculator, wherePhrases.get(0).getWhereExpression());
            case SELECT_FROM_GROUPBY:
                return new GroupByQueryStatement(content, tableName, selectFieldAndCalculator, groupByPhrases.get(0).getGroupByFields());
            case SELECT_FROM_WHERE_GROUPBY:
                return new GroupByQueryStatement(content, tableName, selectFieldAndCalculator,
                        groupByPhrases.get(0).getGroupByFields(), wherePhrases.get(0).getWhereExpression(), ExpressionType.WHERE);
            case SELECT_FROM_GROUPBY_HAVING:
                return new GroupByQueryStatement(content, tableName, selectFieldAndCalculator,
                        groupByPhrases.get(0).getGroupByFields(), havingPhrases.get(0).getHavingExpression(), ExpressionType.HAVING);
            case SELECT_FROM_WHERE_GROUPBY_HAVING:
                return new GroupByQueryStatement(content, tableName, selectFieldAndCalculator,
                        groupByPhrases.get(0).getGroupByFields(), wherePhrases.get(0).getWhereExpression(), havingPhrases.get(0).getHavingExpression());
            case SELECT_FROM_GROUPBY_WINDOW:
                return new WindowQueryStatement(content, tableName, selectFieldAndCalculator,
                        groupByPhrases.get(0).getGroupByFields(), windowInfoInSQLS.get(0));
            case SELECT_FROM_WHERE_GROUPBY_WINDOW:
                return new WindowQueryStatement(content, tableName, selectFieldAndCalculator,
                        groupByPhrases.get(0).getGroupByFields(), windowInfoInSQLS.get(0), wherePhrases.get(0).getWhereExpression(), ExpressionType.WHERE);
            case SELECT_FROM_GROUPBY_WINDOW_HAVING:
                return new WindowQueryStatement(content, tableName, selectFieldAndCalculator,
                        groupByPhrases.get(0).getGroupByFields(), windowInfoInSQLS.get(0), havingPhrases.get(0).getHavingExpression(), ExpressionType.HAVING);
            case SELECT_FROM_WHERE_GROUPBY_WINDOW_HAVING:
                return new WindowQueryStatement(content, tableName, selectFieldAndCalculator,
                        groupByPhrases.get(0).getGroupByFields(), windowInfoInSQLS.get(0), wherePhrases.get(0).getWhereExpression(), havingPhrases.get(0).getHavingExpression());
            case SELECT_FROM_JOIN: {
                assert joinPhrase != null;
                return new JointStatement(content, tableName, selectFieldAndCalculator, joinPhrase.getJoinType(), asSourceTableName,
                        joinPhrase.getJoinTableName(), joinPhrase.getAsJoinTableName(), joinPhrase.getJoinCondition());
            }
            case SELECT_FROM_WHERE_JOIN: {
                assert joinPhrase != null;
                Expression whereExpression = wherePhrases.get(0).getWhereExpression();
                return new JointWhereStatement(content, tableName, selectFieldAndCalculator, joinPhrase.getJoinType(), asSourceTableName,
                        joinPhrase.getJoinTableName(), joinPhrase.getAsJoinTableName(), joinPhrase.getJoinCondition(), whereExpression, true);
            }
            case SELECT_FROM_WHERE_JOIN_WHERE: {
                assert joinPhrase != null;
                return new JointWhereStatement(content, tableName, selectFieldAndCalculator, joinPhrase.getJoinType(), asSourceTableName,
                        joinPhrase.getJoinTableName(), joinPhrase.getAsJoinTableName(), joinPhrase.getJoinCondition(),
                        wherePhrases.get(0).getWhereExpression(), wherePhrases.get(1).getWhereExpression());
            }
            case SELECT_FROM_WHERE_JOIN_WHERE_GROUPBY: {
                assert joinPhrase != null;
                return new JointWhereGroupByStatement(content, tableName, selectFieldAndCalculator, joinPhrase.getJoinType(), asSourceTableName,
                        joinPhrase.getJoinTableName(), joinPhrase.getAsJoinTableName(), joinPhrase.getJoinCondition(),
                        wherePhrases.get(0).getWhereExpression(), wherePhrases.get(1).getWhereExpression(),
                        groupByPhrases.get(0).getGroupByFields());
            }
            case SELECT_FROM_WHERE_JOIN_WHERE_GROUPBY_HAVING: {
                assert joinPhrase != null;
                return new JointWhereGBHavingStatement(content, tableName, selectFieldAndCalculator, joinPhrase.getJoinType(), asSourceTableName,
                        joinPhrase.getJoinTableName(), joinPhrase.getAsJoinTableName(), joinPhrase.getJoinCondition(),
                        wherePhrases.get(0).getWhereExpression(), wherePhrases.get(1).getWhereExpression(),
                        groupByPhrases.get(0).getGroupByFields(), havingPhrases.get(0).getHavingExpression());
            }
            case SELECT_FROM_WHERE_JOIN_GROUPBY: {
                assert joinPhrase != null;
                return new JointWhereGroupByStatement(content, tableName, selectFieldAndCalculator, joinPhrase.getJoinType(), asSourceTableName,
                        joinPhrase.getJoinTableName(), joinPhrase.getAsJoinTableName(), joinPhrase.getJoinCondition(),
                        wherePhrases.get(0).getWhereExpression(), true,
                        groupByPhrases.get(0).getGroupByFields());
            }
            case SELECT_FROM_WHERE_JOIN_GROUPBY_HAVING: {
                assert joinPhrase != null;
                return new JointWhereGBHavingStatement(content, tableName, selectFieldAndCalculator, joinPhrase.getJoinType(), asSourceTableName,
                        joinPhrase.getJoinTableName(), joinPhrase.getAsJoinTableName(), joinPhrase.getJoinCondition(),
                        wherePhrases.get(0).getWhereExpression(), true,
                        groupByPhrases.get(0).getGroupByFields(), havingPhrases.get(0).getHavingExpression());
            }
            case SELECT_FROM_JOIN_WHERE: {
                assert joinPhrase != null;
                Expression whereExpression = wherePhrases.get(0).getWhereExpression();
                return new JointWhereStatement(content, tableName, selectFieldAndCalculator, joinPhrase.getJoinType(), asSourceTableName,
                        joinPhrase.getJoinTableName(), joinPhrase.getAsJoinTableName(), joinPhrase.getJoinCondition(), whereExpression, false);
            }
            case SELECT_FROM_JOIN_WHERE_GROUPBY: {
                assert joinPhrase != null;
                return new JointWhereGroupByStatement(content, tableName, selectFieldAndCalculator, joinPhrase.getJoinType(), asSourceTableName,
                        joinPhrase.getJoinTableName(), joinPhrase.getAsJoinTableName(), joinPhrase.getJoinCondition(),
                        wherePhrases.get(0).getWhereExpression(), false,
                        groupByPhrases.get(0).getGroupByFields());
            }
            case SELECT_FROM_JOIN_WHERE_GROUPBY_HAVING: {
                assert joinPhrase != null;
                return new JointWhereGBHavingStatement(content, tableName, selectFieldAndCalculator, joinPhrase.getJoinType(), asSourceTableName,
                        joinPhrase.getJoinTableName(), joinPhrase.getAsJoinTableName(), joinPhrase.getJoinCondition(),
                        wherePhrases.get(0).getWhereExpression(), false,
                        groupByPhrases.get(0).getGroupByFields(), havingPhrases.get(0).getHavingExpression());
            }
            case SELECT_FROM_JOIN_GROUPBY: {
                assert joinPhrase != null;
                return new JointGroupByStatement(content, tableName, selectFieldAndCalculator, joinPhrase.getJoinType(), asSourceTableName,
                        joinPhrase.getJoinTableName(), joinPhrase.getAsJoinTableName(), joinPhrase.getJoinCondition(), groupByPhrases.get(0).getGroupByFields());
            }
            case SELECT_FROM_JOIN_GROUPBY_HAVING: {
                assert joinPhrase != null;
                return new JointGroupByHavingStatement(content, tableName, selectFieldAndCalculator, joinPhrase.getJoinType(), asSourceTableName,
                        joinPhrase.getJoinTableName(), joinPhrase.getAsJoinTableName(), joinPhrase.getJoinCondition(),
                        groupByPhrases.get(0).getGroupByFields(), havingPhrases.get(0).getHavingExpression());
            }
            default: {
                throw new UnsupportedOperationException(ParserUtil.getText(ctx));
            }

        }
    }

    @Override
    public Node visitCreateTable(SqlParser.CreateTableContext ctx) {
        SqlParser.IdentifierContext identifier = ctx.tableName().identifier();
        StringType tableName = (StringType) visit(identifier);

        SqlParser.TableDescriptorContext tableDescriptor = ctx.tableDescriptor();
        Columns columns = (Columns) visit(tableDescriptor);


        SqlParser.TablePropertiesContext propertiesContext = ctx.tableProperties();
        TableProperties properties = (TableProperties) visit(propertiesContext);

        return new CreateTableStatement(ParserUtil.getText(ctx), tableName.result(), columns, properties.getHolder());
    }

    @Override
    public Node visitCreateView(SqlParser.CreateViewContext ctx) {
        SqlParser.ViewNameContext viewNameContext = ctx.viewName();
        StringType viewTableName = (StringType) visit(viewNameContext.identifier());
        QueryStatement queryStatement = (QueryStatement) visit(ctx.query());
        return new CreateViewStatement(ParserUtil.getText(ctx), viewTableName.result(), queryStatement);
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
            return new InsertQueryStatement(ParserUtil.getText(ctx), tableName, queryStatement);
        } else {
            return new InsertQueryStatement(ParserUtil.getText(ctx), tableName, queryStatement, targetColumns);
        }
    }

    @Override
    public Node visitInsertValue(SqlParser.InsertValueContext ctx) {
        StringType tableNameStringType = (StringType) visit(ctx.tableName());
        String tableName = tableNameStringType.result();

        SqlParser.TableDescriptorContext tableDescriptor = ctx.tableDescriptor();
        Columns targetColumns = null;
        if (tableDescriptor != null) {
            targetColumns = (Columns) visit(tableDescriptor);
        }

        SqlParser.ValuesContext valuesContext = ctx.values();
        MultiLiteral values = (MultiLiteral) visit(valuesContext);

        if (targetColumns != null && targetColumns.getHolder().size() != values.getLiterals().size()) {
            throw new IllegalArgumentException("number of value is not correct.");
        }


        List<ColumnValue> list = new ArrayList<>();
        if (targetColumns != null) {
            for (int i = 0; i < targetColumns.getHolder().size(); i++) {
                Pair<String, FieldType> pair = targetColumns.getHolder().get(i);

                ColumnValue columnValue = new ColumnValue();
                columnValue.setFieldName(pair.getKey());
                columnValue.setFieldType(pair.getValue());
                columnValue.setValue(values.getLiterals().get(i));

                list.add(columnValue);
            }
        } else {
            for (Literal<?> literal : values.getLiterals()) {
                ColumnValue temp = new ColumnValue();
                temp.setValue(literal);
                list.add(temp);
            }
        }

        return new InsertValueStatement(ParserUtil.getText(ctx), tableName, list);
    }

    @Override
    public Node visitWherePhrase(SqlParser.WherePhraseContext ctx) {
        SqlParser.BooleanExpressionContext firstBooleanExpressionContext = ctx.booleanExpression();
        Expression whereExpression = (Expression) visit(firstBooleanExpressionContext);
        return new WherePhrase(ParserUtil.getText(ctx), whereExpression);
    }

    @Override
    public Node visitGroupByPhrase(SqlParser.GroupByPhraseContext ctx) {

        List<SqlParser.FieldNameContext> fieldNameContexts = ctx.fieldName();
        List<Field> groupByFields = fieldNameContexts.stream().map(this::visit).map(target -> (Field) target).collect(Collectors.toList());

        SqlParser.WindowFunctionContext windowFunctionContext = ctx.windowFunction();
        WindowInfoInSQL windowInfoInSQL = null;
        if (windowFunctionContext != null) {
            windowInfoInSQL = (WindowInfoInSQL) visit(windowFunctionContext);
        }

        return new GroupByPhrase(ParserUtil.getText(ctx), groupByFields, windowInfoInSQL);
    }

    @Override
    public Node visitHavingPhrase(SqlParser.HavingPhraseContext ctx) {
        SqlParser.BooleanExpressionContext secondBooleanExpressionContext = ctx.booleanExpression();

        Expression havingExpression = (Expression) visit(secondBooleanExpressionContext);

        return new HavingPhrase(ParserUtil.getText(ctx), havingExpression);
    }

    @Override
    public Node visitJoinPhrase(SqlParser.JoinPhraseContext ctx) {
        JoinType joinType;
        TerminalNode terminalNode = ctx.LEFT();
        if (terminalNode != null) {
            joinType = JoinType.LEFT_JOIN;
        } else {
            joinType = JoinType.INNER_JOIN;
        }

        String joinTableName = ParserUtil.getText(ctx.tableName());

        Object tableNameAsJoin = null;
        SqlParser.IdentifierContext identifier = ctx.identifier();
        if (identifier != null) {
            tableNameAsJoin = visit(identifier);
        }

        String asJoinTableName = null;
        if (tableNameAsJoin != null) {
            StringType temp = (StringType) tableNameAsJoin;
            asJoinTableName = temp.result();
        }

        SqlParser.JoinConditionContext joinConditionContext = ctx.joinCondition();
        JoinCondition joinCondition = (JoinCondition) visit(joinConditionContext);

        return new JoinPhrase(ParserUtil.getText(ctx), joinType, joinTableName, asJoinTableName, joinCondition);
    }

    @Override
    public Node visitSelectField(SqlParser.SelectFieldContext ctx) {
        List<SelectFieldResult> result = new ArrayList<>();

        List<SqlParser.AsFieldContext> asFieldContexts = ctx.asField();

        if (asFieldContexts != null && asFieldContexts.size() != 0) {

            List<WindowInfoInSQL> windowInfoInSQLS = new ArrayList<>();

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

                if (item instanceof WindowInfoInSQL) {
                    WindowInfoInSQL windowInfoInSQL = (WindowInfoInSQL) item;
                    windowInfoInSQLS.add(windowInfoInSQL);
                }
            }


            String windowStartFieldName = null;
            String windowEndFieldName = null;
            Field timestampField = null;

            for (WindowInfoInSQL info : windowInfoInSQLS) {
                if (info.getFirstWordInSQL() == WindowInfoInSQL.FirstWordInSQL.WINDOW_START) {
                    windowStartFieldName = info.getNewFieldName();
                } else if (info.getFirstWordInSQL() == WindowInfoInSQL.FirstWordInSQL.WINDOW_END) {
                    windowEndFieldName = info.getNewFieldName();
                }

                if (timestampField == null) {
                    timestampField = info.getTimeField();
                } else if (timestampField.equals(info.getTimeField())) {
                    throw new SyntaxErrorException("window_start，window_end时间戳必须来自同一字段");
                }
            }

            for (WindowInfoInSQL windowInfoInSQL : windowInfoInSQLS) {

                SelectWindowResult selectWindowResult;
                if (windowInfoInSQL.getFirstWordInSQL() == WindowInfoInSQL.FirstWordInSQL.WINDOW_START) {
                    selectWindowResult = new SelectWindowResult(windowInfoInSQL.getTimeField(), Calculator.WINDOW_START, windowStartFieldName, windowEndFieldName);
                } else if (windowInfoInSQL.getFirstWordInSQL() == WindowInfoInSQL.FirstWordInSQL.WINDOW_END) {
                    selectWindowResult = new SelectWindowResult(windowInfoInSQL.getTimeField(), Calculator.WINDOW_END, windowStartFieldName, windowEndFieldName);
                } else {
                    throw new RSQLServerException("unknown type=" + windowInfoInSQL.getFirstWordInSQL());
                }


                //用于校验
                selectWindowResult.setWindowInfo(windowInfoInSQL);
                result.add(selectWindowResult);
            }

        } else {
            String starText = ctx.STAR().getText();
            //默认是null，需要选择原表中所有字段；
            Field field = new Field(starText, "", starText);
            SelectFieldResult fieldResult = new SelectFieldResult(field);
            result.add(fieldResult);
        }

        return new ListNode<SelectFieldResult>(ParserUtil.getText(ctx), result);
    }

    @Override
    public Node visitJoinCondition(SqlParser.JoinConditionContext ctx) {
        List<JoinCondition> visit = this.visit(ctx.oneJoinCondition(), JoinCondition.class);

        JoinCondition condition = new JoinCondition(ParserUtil.getText(ctx));
        for (JoinCondition joinCondition : visit) {
            condition.addJoinCondition(joinCondition);
        }

        return condition;
    }

    @Override
    public Node visitOneJoinCondition(SqlParser.OneJoinConditionContext ctx) {
        SqlParser.FieldNameContext leftFieldNameContext = ctx.fieldName().get(0);
        SqlParser.FieldNameContext rightFieldNameContext = ctx.fieldName().get(1);

        Field leftField = (Field) visit(leftFieldNameContext);
        Field rightField = (Field) visit(rightFieldNameContext);

        assert ctx.EQUAL_SYMBOL().getSymbol().getType() == SqlParser.EQUAL_SYMBOL;

        JoinCondition condition = new JoinCondition(ParserUtil.getText(ctx));
        condition.addField(leftField, rightField);

        return condition;
    }

    @Override
    public Node visitTableDescriptor(SqlParser.TableDescriptorContext ctx) {
        List<SqlParser.ColumnDescriptorContext> list = ctx.columnDescriptor();
        List<Columns> temp = this.visit(list, Columns.class);

        Columns columns = new Columns(ParserUtil.getText(ctx));

        for (Columns item : temp) {
            columns.addColumns(item.getHolder());
        }

        return columns;
    }

    @Override
    public Node visitColumnDescriptor(SqlParser.ColumnDescriptorContext ctx) {

        Columns columns = new Columns(ParserUtil.getText(ctx));

        SqlParser.IdentifierContext identifierContext = ctx.identifier();
        StringType columnName = (StringType) visit(identifierContext);

        FieldType type = null;
        SqlParser.DataTypeContext dataTypeContext = ctx.dataType();
        if (dataTypeContext != null) {
            StringType columnType = (StringType) visit(dataTypeContext);

            type = FieldType.getByType(columnType.result());
        }

        columns.addFieldNameAndType(columnName.result(), type);

        return columns;
    }

    @Override
    public Node visitTableProperties(SqlParser.TablePropertiesContext ctx) {
        List<SqlParser.TablePropertyContext> tablePropertyContexts = ctx.tableProperty();
        List<TableProperties> temp = this.visit(tablePropertyContexts, TableProperties.class);

        TableProperties result = new TableProperties(ParserUtil.getText(ctx));
        for (TableProperties tableProperties : temp) {
            result.addProperties(tableProperties.getHolder());
        }

        return result;
    }

    @Override
    public Node visitTableProperty(SqlParser.TablePropertyContext ctx) {

        SqlParser.IdentifierContext identifier = ctx.identifier();

        StringType temp = (StringType) visit(identifier);
        String key = temp.result();


        SqlParser.ValueContext value = ctx.value();
        Literal<?> result = (Literal<?>) visit(value);

        TableProperties tableProperties = new TableProperties(ParserUtil.getText(ctx));
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
        WindowInfoInSQL windowInfoInSQL = (WindowInfoInSQL) visit(ctx.windowFunction());

        String newFieldName = getIdentifier(ctx.identifier());
        windowInfoInSQL.getTimeField().setAsFieldName(newFieldName);
        windowInfoInSQL.setNewFieldName(newFieldName);

        return windowInfoInSQL;
    }

    @Override
    public Node visitFunction(SqlParser.FunctionContext ctx) {
        String text = ctx.calculator().getText();
        Calculator calculator = Calculator.valueOf(text.toUpperCase());

        SqlParser.FieldNameContext fieldNameContext = ctx.fieldName();
        if (fieldNameContext != null) {
            Field field = (Field) visit(fieldNameContext);
            return new Function(ParserUtil.getText(ctx), calculator, field);
        }

        String star = ctx.STAR().getText();
        if (!StringUtils.isEmpty(star)) {
            Field field = new Field(ParserUtil.getText(ctx), RSQLConstant.STAR);
            return new Function(ParserUtil.getText(ctx), calculator, field);
        }

        throw new IllegalArgumentException("parser function error: " + ParserUtil.getText(ctx));
    }

    //一个sql会返回三个windowInfo
    @Override
    public Node visitTumbleWindow(SqlParser.TumbleWindowContext ctx) {
        SqlParser.Tumble_windowContext tumbleWindowContext = ctx.tumble_window();

        String tumble = null;
        if (tumbleWindowContext.TUMBLE() != null) {
            tumble = tumbleWindowContext.TUMBLE().getText();
        }
        String tumbleStart = null;
        if (tumbleWindowContext.TUMBLE_START() != null) {
            tumbleStart = tumbleWindowContext.TUMBLE_START().getText();
        }

        String tumbleEnd = null;
        if (tumbleWindowContext.TUMBLE_END() != null) {
            tumbleEnd = tumbleWindowContext.TUMBLE_END().getText();
        }

        Field field = (Field) visit(tumbleWindowContext.fieldName());
        String time = ParserUtil.getLiteralText(tumbleWindowContext.QUOTED_NUMBER());
        long size = Long.parseLong(time);

        TimeUnit timeUnit = ParserUtil.getTimeUnit(tumbleWindowContext.timeunit().getText());

        assert timeUnit != null;
        long secondSize = timeUnit.toSeconds(size);

        WindowInfoInSQL windowInfoInSQL = new WindowInfoInSQL(ParserUtil.getText(ctx), WindowInfoInSQL.WindowType.TUMBLE, secondSize, secondSize, field);
        if (!StringUtils.isEmpty(tumbleStart)) {
            windowInfoInSQL.setFirstWordInSQL(WindowInfoInSQL.FirstWordInSQL.WINDOW_START);
        } else if (!StringUtils.isEmpty(tumbleEnd)) {
            windowInfoInSQL.setFirstWordInSQL(WindowInfoInSQL.FirstWordInSQL.WINDOW_END);
        } else if (!StringUtils.isEmpty(tumble)) {
            windowInfoInSQL.setFirstWordInSQL(WindowInfoInSQL.FirstWordInSQL.WINDOW);
        }

        return windowInfoInSQL;
    }

    @Override
    public Node visitHopWindow(SqlParser.HopWindowContext ctx) {
        SqlParser.Hop_windowContext hopWindowContext = ctx.hop_window();

        String hop = null;
        if (hopWindowContext.HOP() != null) {
            hop = hopWindowContext.HOP().getText();
        }
        String hopStart = null;
        if (hopWindowContext.HOP_START() != null) {
            hopStart = hopWindowContext.HOP_START().getText();
        }

        String hopEnd = null;
        if (hopWindowContext.HOP_END() != null) {
            hopEnd = hopWindowContext.HOP_END().getText();
        }

        Field field = (Field) visit(hopWindowContext.fieldName());


        String slideSize = ParserUtil.getLiteralText(hopWindowContext.QUOTED_NUMBER(0));
        TimeUnit slideTimeUnit = ParserUtil.getTimeUnit(hopWindowContext.timeunit(0).getText());
        long slide = Long.parseLong(slideSize);

        String windowSize = ParserUtil.getLiteralText(hopWindowContext.QUOTED_NUMBER(1));
        TimeUnit windowTimeUnit = ParserUtil.getTimeUnit(hopWindowContext.timeunit(1).getText());
        long size = Long.parseLong(windowSize);


        assert windowTimeUnit != null;
        long secondSize = windowTimeUnit.toSeconds(size);
        assert slideTimeUnit != null;
        long secondSlide = slideTimeUnit.toSeconds(slide);

        WindowInfoInSQL windowInfoInSQL = new WindowInfoInSQL(ParserUtil.getText(ctx), WindowInfoInSQL.WindowType.HOP, secondSlide, secondSize, field);
        if (!StringUtils.isEmpty(hopStart)) {
            windowInfoInSQL.setFirstWordInSQL(WindowInfoInSQL.FirstWordInSQL.WINDOW_START);
        } else if (!StringUtils.isEmpty(hopEnd)) {
            windowInfoInSQL.setFirstWordInSQL(WindowInfoInSQL.FirstWordInSQL.WINDOW_END);
        } else if (!StringUtils.isEmpty(hop)) {
            windowInfoInSQL.setFirstWordInSQL(WindowInfoInSQL.FirstWordInSQL.WINDOW);
        }

        return windowInfoInSQL;
    }

    @Override
    public Node visitSessionWindow(SqlParser.SessionWindowContext ctx) {
        SqlParser.Session_windowContext sessionWindowContext = ctx.session_window();

        String session = null;
        if (sessionWindowContext.SESSION() != null) {
            session = sessionWindowContext.SESSION().getText();
        }
        String sessionStart = null;
        if (sessionWindowContext.SESSION_START() != null) {
            sessionStart = sessionWindowContext.SESSION_START().getText();
        }

        String sessionEnd = null;
        if (sessionWindowContext.SESSION_END() != null) {
            sessionEnd = sessionWindowContext.SESSION_END().getText();
        }

        Field field = (Field) visit(sessionWindowContext.fieldName());
        String num = ParserUtil.getLiteralText(sessionWindowContext.QUOTED_NUMBER());
        TimeUnit timeUnit = ParserUtil.getTimeUnit(sessionWindowContext.timeunit().getText());
        //todo 异常体系
        long size = Long.parseLong(num);

        assert timeUnit != null;
        long secondSize = timeUnit.toSeconds(size);

        WindowInfoInSQL windowInfoInSQL = new WindowInfoInSQL(ParserUtil.getText(ctx), WindowInfoInSQL.WindowType.SESSION, secondSize, secondSize, field);
        if (!StringUtils.isEmpty(sessionStart)) {
            windowInfoInSQL.setFirstWordInSQL(WindowInfoInSQL.FirstWordInSQL.WINDOW_START);
        } else if (!StringUtils.isEmpty(sessionEnd)) {
            windowInfoInSQL.setFirstWordInSQL(WindowInfoInSQL.FirstWordInSQL.WINDOW_END);
        } else if (!StringUtils.isEmpty(session)) {
            windowInfoInSQL.setFirstWordInSQL(WindowInfoInSQL.FirstWordInSQL.WINDOW);
        }

        return windowInfoInSQL;
    }

    @Override
    public Node visitJointExpression(SqlParser.JointExpressionContext ctx) {
        SqlParser.BooleanExpressionContext leftExpressionContext = ctx.booleanExpression(0);
        SqlParser.BooleanExpressionContext rightExpressionContext = ctx.booleanExpression(1);


        Expression left = (Expression) visit(leftExpressionContext);
        Expression right = (Expression) visit(rightExpressionContext);

        if (ctx.AND().getSymbol().getType() == SqlParser.AND) {
            return new AndExpression(ParserUtil.getText(ctx), left, right);
        }

        if (ctx.OR().getSymbol().getType() == SqlParser.OR) {
            return new OrExpression(ParserUtil.getText(ctx), left, right);
        }


        throw new IllegalArgumentException("unrecognizable sql: " + ParserUtil.getText(ctx));
    }

    @Override
    public Node visitOperatorExpression(SqlParser.OperatorExpressionContext ctx) {
        Field field = (Field) visit(ctx.fieldName());

        String operatorText = ctx.operator().getText();
        Operator operator = ParserUtil.getOperator(operatorText);

        Node node = visit(ctx.value());
        if (node == null) {
            return new SingleValueExpression(ParserUtil.getText(ctx), field, operator, null);
        } else {
            return new SingleValueExpression(ParserUtil.getText(ctx), field, operator, (Literal<?>) node);
        }

    }

    @Override
    public Node visitIsNullExpression(SqlParser.IsNullExpressionContext ctx) {
        Field field = (Field) visit(ctx.fieldName());

        assert ctx.IS().getSymbol().getType() == SqlParser.IS;
        assert ctx.NULL().getSymbol().getType() == SqlParser.NULL;

        return new SingleValueExpression(ParserUtil.getText(ctx), field, Operator.EQUAL, null);
    }

    @Override
    public Node visitBetweenExpression(SqlParser.BetweenExpressionContext ctx) {
        Field field = (Field) visit(ctx.fieldName());

        String low = ctx.NUMBER(0).getText();
        String high = ctx.NUMBER(1).getText();


        return new RangeValueExpression(ParserUtil.getText(ctx), field, Long.parseLong(low), Long.parseLong(high));
    }

    @Override
    public Node visitInExpression(SqlParser.InExpressionContext ctx) {
        Field field = (Field) visit(ctx.fieldName());
        if (ctx.IN().getSymbol().getType() != SqlParser.IN) {
            throw new SyntaxErrorException("operator must be in.");
        }

        MultiLiteral visit = (MultiLiteral) visit(ctx.values());

        return new MultiValueExpression(ParserUtil.getText(ctx), field, visit);
    }

    @Override
    public Node visitFunctionExpression(SqlParser.FunctionExpressionContext ctx) {
        Function function = (Function) visit(ctx.function());

        String operatorText = ctx.operator().getText();
        Operator operator = ParserUtil.getOperator(operatorText);

        Node node = visit(ctx.value());
        if (node == null) {
            return new SingleValueCalcuExpression(ParserUtil.getText(ctx), function.getField(), operator, null, function.getCalculator());
        } else {
            Literal<?> literal = (Literal<?>) node;
            return new SingleValueCalcuExpression(ParserUtil.getText(ctx), function.getField(), operator, literal, function.getCalculator());
        }
    }

    @Override
    public Node visitFieldName(SqlParser.FieldNameContext ctx) {
        String tableName = null;
        String fieldName = null;

        SqlParser.TableNameContext tableNameContext = ctx.tableName();
        if (tableNameContext != null) {
            StringType tableNameType = (StringType) visit(tableNameContext);
            StringType fieldNameType = (StringType) visit(ctx.identifier());
            fieldName = fieldNameType.getLiteral();
            tableName = tableNameType.result();
        } else {
            StringType fieldNameType = (StringType) visit(ctx.identifier());
            fieldName = fieldNameType.getLiteral();
        }
        return new Field(ParserUtil.getText(ctx), tableName, fieldName);
    }

    @Override
    public Node visitErrorNode(ErrorNode node) {
        ParseTree parent = node.getParent();
        if (!(parent instanceof SqlParser.IdentifierContext)) {
            return super.visitErrorNode(node);
        }

        SqlParser.IdentifierContext context = (SqlParser.IdentifierContext) parent;
        RecognitionException recognitionException = context.exception;
        if (!(recognitionException instanceof InputMismatchException)) {
            return super.visitErrorNode(node);
        }

        String str = ParserUtil.getText(context);
        if (ParserUtil.isKeyWord(str)) {
            logger.info("【visitErrorNode】identifier is key word, that is ok, identifier:{}.", str);
            return new StringType(str, str);
        }

        return super.visitErrorNode(node);
    }

    @Override
    public Node visitAlphabetIdentifier(SqlParser.AlphabetIdentifierContext ctx) {
        String text = ctx.ALPHABET_STRING().getText();
        return new StringType(ParserUtil.getText(ctx), text);
    }

    @Override
    public Node visitQuotedIdentifier(SqlParser.QuotedIdentifierContext ctx) {
        String text = ctx.QUOTED_STRING().getText();

        if (text == null) {
            return null;
        }

        text = text.substring(1, text.length() - 1);

        return new StringType(ParserUtil.getText(ctx), text);
    }

    @Override
    public Node visitBackQuotedIdentifier(SqlParser.BackQuotedIdentifierContext ctx) {
        String text = ctx.BACKQUOTED_STRING().getText();
        if (text == null) {
            return null;
        }

        text = text.substring(1, text.length() - 1);
        return new StringType(ParserUtil.getText(ctx), text);
    }

    @Override
    public Node visitNumIdentifier(SqlParser.NumIdentifierContext ctx) {
        String text = ctx.NUM_STRING().getText();

        if (text == null) {
            return null;
        }

        text = text.substring(1, text.length() - 1);
        return new StringType(ParserUtil.getText(ctx), text);
    }

    @Override
    public Node visitStringIdentifier(SqlParser.StringIdentifierContext ctx) {
        String text = ctx.STRING().getText();

        if (text == null) {
            return null;
        }

        text = text.substring(1, text.length() - 1);
        return new StringType(ParserUtil.getText(ctx), text);
    }

    @Override
    public Node visitVariable(SqlParser.VariableContext ctx) {
        throw new UnsupportedOperationException(ParserUtil.getText(ctx));
    }

    @Override
    public Node visitValues(SqlParser.ValuesContext ctx) {
        List<SqlParser.ValueContext> literals = ctx.value();
        List<Literal<?>> values = literals.stream().map(this::visit).map(value -> (Literal<?>) value).collect(Collectors.toList());
        return new MultiLiteral(ParserUtil.getText(ctx), values);
    }

    @Override
    public Node visitNullValue(SqlParser.NullValueContext ctx) {
        return new StringType(ParserUtil.getText(ctx), null);
    }



    @Override
    public Node visitBooleanValue(SqlParser.BooleanValueContext ctx) {
        String falseBoolean = ctx.FALSE().getText();
        String trueBoolean = ctx.TRUE().getText();

        boolean value;
        if (falseBoolean != null) {
            value = Boolean.parseBoolean(falseBoolean);
        } else {
            value = Boolean.parseBoolean(trueBoolean);
        }

        return new BooleanType(ParserUtil.getText(ctx), value);
    }

    @Override
    public Node visitNumberValue(SqlParser.NumberValueContext ctx) {
        String text = ctx.NUMBER().getText();
        Number result = 0;
        if (text.contains(".")) {
            result = Double.valueOf(text);
        } else {
            result = Long.valueOf(text);
        }
        return new NumberType(ParserUtil.getText(ctx), result);
    }

    @Override
    public Node visitStringValue(SqlParser.StringValueContext ctx) {
        String text = ctx.STRING().getText();

        if (text == null) {
            return null;
        }

        text = text.substring(1, text.length() - 1);

        return new StringType(ParserUtil.getText(ctx), text);
    }

    @Override
    public Node visitVariableValue(SqlParser.VariableValueContext ctx) {
        throw new UnsupportedOperationException(ParserUtil.getText(ctx));
    }

    @Override
    public Node visitQuotedNumberValue(SqlParser.QuotedNumberValueContext ctx) {
        String text = ctx.QUOTED_NUMBER().getText();
        if (text == null) {
            return null;
        }

        text = text.substring(1, text.length() - 1);

        return new StringType(ParserUtil.getText(ctx), text);
    }

    @Override
    public Node visitQuotedStringValue(SqlParser.QuotedStringValueContext ctx) {
        String text = ctx.QUOTED_STRING().getText();
        if (text == null) {
            return null;
        }

        text = text.substring(1, text.length() - 1);

        return new StringType(ParserUtil.getText(ctx), text);
    }

    @Override
    public Node visitBackQuotedStringValue(SqlParser.BackQuotedStringValueContext ctx) {
        String text = ctx.BACKQUOTED_STRING().getText();

        if (text == null) {
            return null;
        }

        text = text.substring(1, text.length() - 1);

        return new StringType(ParserUtil.getText(ctx), text);
    }

    private String getIdentifier(SqlParser.IdentifierContext identifier) {
        if (identifier != null) {
            Literal<String> literal = (Literal<String>) visit(identifier);
            return literal.result();
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
