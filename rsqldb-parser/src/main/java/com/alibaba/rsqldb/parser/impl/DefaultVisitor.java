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

import com.alibaba.rsqldb.parser.SqlParser;
import com.alibaba.rsqldb.parser.SqlParserBaseVisitor;
import com.alibaba.rsqldb.parser.pojo.Calculator;
import com.alibaba.rsqldb.parser.pojo.Column;
import com.alibaba.rsqldb.parser.pojo.Expression;
import com.alibaba.rsqldb.parser.pojo.Field;
import com.alibaba.rsqldb.parser.pojo.FieldType;
import com.alibaba.rsqldb.parser.pojo.Function;
import com.alibaba.rsqldb.parser.pojo.Insert;
import com.alibaba.rsqldb.parser.pojo.Operator;
import com.alibaba.rsqldb.parser.pojo.Table;
import com.alibaba.rsqldb.parser.pojo.ColumnValue;
import com.alibaba.rsqldb.parser.pojo.WindowInfo;
import com.alibaba.rsqldb.parser.pojo.expression.AndExpression;
import com.alibaba.rsqldb.parser.pojo.expression.MultiValueExpression;
import com.alibaba.rsqldb.parser.pojo.expression.OrExpression;
import com.alibaba.rsqldb.parser.pojo.expression.RangeValueExpression;
import com.alibaba.rsqldb.parser.pojo.expression.SingleValueExpression;
import com.alibaba.rsqldb.parser.util.Pair;
import com.alibaba.rsqldb.parser.util.ParserUtil;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@SuppressWarnings("unchecked")
public class DefaultVisitor extends SqlParserBaseVisitor<Object> {
    @Override
    public Object visitSqlStatements(SqlParser.SqlStatementsContext ctx) {
        List<SqlParser.SqlStatementContext> sqlStatementContexts = ctx.sqlStatement();

        List<Object> collect = sqlStatementContexts.stream().map(this::visit).collect(Collectors.toList());

        System.out.println("all over");

        return collect;
    }

    @Override
    public Object visitSqlStatement(SqlParser.SqlStatementContext ctx) {
        return visit(ctx.sqlBody());
    }

    @Override
    public Object visitQueryStatement(SqlParser.QueryStatementContext ctx) {
        return visit(ctx.query());
    }

    @Override
    public Object visitQuery(SqlParser.QueryContext ctx) {
        SqlParser.SelectFieldContext selectFieldContext = ctx.selectField();
        List<SqlParser.AsFieldContext> asFieldContexts = selectFieldContext.asField();
        String starText = selectFieldContext.STAR().getText();

        String tableName = ParserUtil.getText(ctx.tableName(0));
        Object tableAsName = visit(ctx.identifier(0));

        String sourceNewTable;
        if (tableAsName != null) {
            sourceNewTable = (String) tableAsName;
        }

        if (!StringUtils.isEmpty(ctx.WHERE().getText())) {
            SqlParser.BooleanExpressionContext firstBooleanExpressionContext = ctx.booleanExpression(0);
            if (firstBooleanExpressionContext != null) {
                Expression firstExpression = (Expression) visit(firstBooleanExpressionContext);
            }
        }

        if (!StringUtils.isEmpty(ctx.GROUP().getText()) && !StringUtils.isEmpty(ctx.BY().getText())) {
            SqlParser.WindowFunctionContext windowFunctionContext = ctx.windowFunction();
            List<SqlParser.FieldNameContext> fieldNameContexts = ctx.fieldName();
            if (fieldNameContexts != null && fieldNameContexts.size() != 0) {

            }
        }

        if (!StringUtils.isEmpty(ctx.HAVING().getText())) {
            SqlParser.BooleanExpressionContext secondBooleanExpressionContext = ctx.booleanExpression(0);
            if (secondBooleanExpressionContext != null) {
                Expression secondExpression = (Expression) visit(secondBooleanExpressionContext);

            }
        }

        if (!StringUtils.isEmpty(ctx.JOIN().getText())) {
            String leftJoin = ctx.LEFT().getText();
            String joinTableName = ParserUtil.getText(ctx.tableName(1));
            SqlParser.IdentifierContext secondIdentifier = ctx.identifier(1);
            SqlParser.JoinConditionContext joinConditionContext = ctx.joinCondition();

        }

        return null;
    }

    @Override
    public Object visitCreateTable(SqlParser.CreateTableContext ctx) {
        String tableName = ctx.tableName().getText();

        SqlParser.TableDescriptorContext tableDescriptor = ctx.tableDescriptor();
        List<Column> columns = (List<Column>) visit(tableDescriptor);


        SqlParser.TablePropertiesContext propertiesContext = ctx.tableProperties();
        List<Pair<String, String>> properties = (List<Pair<String, String>>) visit(propertiesContext);

        Table result = new Table();

        result.setTableName(tableName);
        result.setColumns(columns);
        result.setProperties(properties);

        return result;
    }

    @Override
    public Object visitTableDescriptor(SqlParser.TableDescriptorContext ctx) {
        List<SqlParser.ColumnDescriptorContext> list = ctx.columnDescriptor();
        List<Column> collect = list.stream().map(this::visit).map(o -> (Column) o).collect(Collectors.toList());

        return collect;
    }

    @Override
    public Object visitColumnDescriptor(SqlParser.ColumnDescriptorContext ctx) {
        System.out.println("visitColumnDescriptor");

        Column column = new Column();

        SqlParser.IdentifierContext identifierContext = ctx.identifier();
        String columnName = (String) visit(identifierContext);

        column.setName(columnName);

        SqlParser.DataTypeContext dataTypeContext = ctx.dataType();
        if (dataTypeContext != null) {
            String columnType = (String) visit(dataTypeContext);

            FieldType type = FieldType.getByType(columnType);
            column.setType(type);
        }

        return column;
    }

    @Override
    public Object visitTableProperties(SqlParser.TablePropertiesContext ctx) {
        List<SqlParser.TablePropertyContext> tablePropertyContexts = ctx.tableProperty();
        return tablePropertyContexts.stream().map(this::visit).collect(Collectors.toList());
    }

    @Override
    public Object visitTableProperty(SqlParser.TablePropertyContext ctx) {
        System.out.println("visitTableProperty");

        SqlParser.IdentifierContext identifier = ctx.identifier();
        TerminalNode string = ctx.STRING();

        String key;
        if (identifier != null) {
            key = (String) visit(identifier);
        } else {
            key = string.getText();
        }

        SqlParser.LiteralContext literal = ctx.literal();
        Object temp = visit(literal);

        Pair<String, String> pair;
        if (temp != null) {
            String value = (String) temp;
            pair = new Pair<>(key, value);
        } else {
            pair = new Pair<>(key, null);
        }

        return pair;
    }

    @Override
    public Object visitInsertValue(SqlParser.InsertValueContext ctx) {
        String tableName = ctx.tableName().getText();

        SqlParser.TableDescriptorContext tableDescriptor = ctx.tableDescriptor();
        List<Column> targetColumns = null;
        if (tableDescriptor != null) {
            targetColumns = (List<Column>) visit(tableDescriptor);
        }

        SqlParser.ValuesContext valuesContext = ctx.values();
        List<String> values = (List<String>) visit(valuesContext);

        if (targetColumns != null && targetColumns.size() != values.size()) {
            throw new IllegalArgumentException("number of value is not correct.");
        }

        Insert insert = new Insert();
        insert.setName(tableName);

        List<ColumnValue> list = new ArrayList<>();
        if (targetColumns != null) {
            for (int i = 0; i < targetColumns.size(); i++) {
                ColumnValue columnValue = new ColumnValue();
                columnValue.setColumn(targetColumns.get(i));
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

        insert.setColumns(list);

        return insert;
    }

    @Override
    public Object visitInsertSelect(SqlParser.InsertSelectContext ctx) {
        String tableName = ctx.tableName().getText();

        SqlParser.QueryContext query = ctx.query();
        Object visit = visit(query);

        return null;
    }

    @Override
    public Object visitAsFieldName(SqlParser.AsFieldNameContext ctx) {
        Field field = (Field) visit(ctx.fieldName());

        String newFieldName = getIdentifier(ctx.identifier());

        field.setAsFieldName(newFieldName);
        return field;
    }

    @Override
    public Object visitAsFunctionField(SqlParser.AsFunctionFieldContext ctx) {
        Function function = (Function) visit(ctx.function());
        String newFieldName = getIdentifier(ctx.identifier());

        function.getField().setAsFieldName(newFieldName);

        return function;
    }

    @Override
    public Object visitAsWindowFunctionField(SqlParser.AsWindowFunctionFieldContext ctx) {
        WindowInfo windowInfo = (WindowInfo) visit(ctx.windowFunction());

        String newFieldName = getIdentifier(ctx.identifier());

        return null;
    }

    @Override
    public Object visitFunction(SqlParser.FunctionContext ctx) {
        String text = ctx.calculator().getText();
        Calculator calculator = Calculator.valueOf(text);

        SqlParser.FieldNameContext fieldNameContext = ctx.fieldName();
        if (fieldNameContext != null) {
            Field field = (Field) visit(fieldNameContext);
            return new Function(calculator, field);
        }

        String star = ctx.STAR().getText();
        if (!StringUtils.isEmpty(star)) {
            return new Function(calculator, true);
        }

        throw new IllegalArgumentException("parser function error: " + ParserUtil.getText(ctx));
    }

    @Override
    public Object visitTumbleWindow(SqlParser.TumbleWindowContext ctx) {
        SqlParser.Tumble_windowContext tumbleWindowContext = ctx.tumble_window();

        String tumble = tumbleWindowContext.TUMBLE().getText();
        String tumbleStart = tumbleWindowContext.TUMBLE_START().getText();
        String tumbleEnd = tumbleWindowContext.TUMBLE_END().getText();

        Field field = (Field) visit(tumbleWindowContext.fieldName());
        String time = (String) visit(tumbleWindowContext.QUOTED_NUMBER());
        long size = Long.parseLong(time);
        TimeUnit timeUnit = ParserUtil.getTimeUnit(tumbleWindowContext.timeunit().getText());


        WindowInfo windowInfo = new WindowInfo(WindowInfo.WindowType.TUMBLE, size, size, timeUnit, field);
        if (!StringUtils.isEmpty(tumbleStart)) {
            windowInfo.setTargetTime(WindowInfo.TargetTime.WINDOW_START);
        } else if (!StringUtils.isEmpty(tumbleEnd)) {
            windowInfo.setTargetTime(WindowInfo.TargetTime.WINDOW_END);
        } else if (!StringUtils.isEmpty(tumble)) {
        }

        return windowInfo;
    }

    @Override
    public Object visitHopWindow(SqlParser.HopWindowContext ctx) {
        SqlParser.Hop_windowContext hopWindowContext = ctx.hop_window();

        String text = hopWindowContext.HOP().getText();
        String hopStart = hopWindowContext.HOP_START().getText();
        String hopEnd = hopWindowContext.HOP_END().getText();

        Field field = (Field) visit(hopWindowContext.fieldName());


        String slideSize = (String) visit(hopWindowContext.QUOTED_NUMBER(0));
        TimeUnit slideTimeUnit = ParserUtil.getTimeUnit(hopWindowContext.timeunit(0).getText());
        String windowSize = (String) visit(hopWindowContext.QUOTED_NUMBER(1));
        TimeUnit windowTimeUnit = ParserUtil.getTimeUnit(hopWindowContext.timeunit(1).getText());


        return null;
    }

    @Override
    public Object visitSessionWindow(SqlParser.SessionWindowContext ctx) {
        SqlParser.Session_windowContext sessionWindowContext = ctx.session_window();

        String session = sessionWindowContext.SESSION().getText();
        String sessionStart = sessionWindowContext.SESSION_START().getText();
        String sessionEnd = sessionWindowContext.SESSION_END().getText();

        Field field = (Field) visit(sessionWindowContext.fieldName());
        String time = (String) visit(sessionWindowContext.QUOTED_NUMBER());
        TimeUnit timeUnit = ParserUtil.getTimeUnit(sessionWindowContext.timeunit().getText());

        return null;
    }

    @Override
    public Object visitJointExpression(SqlParser.JointExpressionContext ctx) {
        SqlParser.BooleanExpressionContext leftExpressionContext = ctx.booleanExpression(0);
        SqlParser.BooleanExpressionContext rightExpressionContext = ctx.booleanExpression(1);


        Expression left = (Expression) visit(leftExpressionContext);
        Expression right = (Expression) visit(rightExpressionContext);

        if (ctx.AND().getSymbol().getType() == SqlParser.AND) {
            return new AndExpression(left, right);
        }

        if (ctx.OR().getSymbol().getType() == SqlParser.OR) {
            return new OrExpression(left, right);
        }


        return new IllegalArgumentException("unrecognizable sql: " + ParserUtil.getText(ctx));
    }

    @Override
    public Object visitOperatorExpression(SqlParser.OperatorExpressionContext ctx) {
        Field field = (Field) visit(ctx.fieldName());

        String operatorText = ctx.operator().getText();
        Operator operator = ParserUtil.getOperator(operatorText);

        String value = (String) visit(ctx.literal());

        return new SingleValueExpression(field, operator, value);
    }

    @Override
    public Object visitIsNullExpression(SqlParser.IsNullExpressionContext ctx) {
        Field field = (Field) visit(ctx.fieldName());

        assert ctx.IS().getSymbol().getType() == SqlParser.IS;
        assert ctx.NULL().getSymbol().getType() == SqlParser.NULL;

        return new SingleValueExpression(field, Operator.EQUAL, null);
    }

    @Override
    public Object visitBetweenExpression(SqlParser.BetweenExpressionContext ctx) {
        Field field = (Field) visit(ctx.fieldName());

        String low = ctx.NUMBER(0).getText();
        String high = ctx.NUMBER(1).getText();


        return new RangeValueExpression(field, Operator.BETWEEN_AND, Long.parseLong(low), Long.parseLong(high));
    }

    @Override
    public Object visitInExpression(SqlParser.InExpressionContext ctx) {
        Field field = (Field) visit(ctx.fieldName());
        assert ctx.IN().getSymbol().getType() == SqlParser.IN;
        List<String> visit = (List<String>) visit(ctx.values());

        return new MultiValueExpression(field, Operator.IN, visit);
    }

    @Override
    public Object visitFieldName(SqlParser.FieldNameContext ctx) {
        String tableName = null;
        String fieldName = null;

        Object identifier = visit(ctx.identifier());
        if (identifier != null) {
            fieldName = (String) identifier;
        } else {
            tableName = (String) visit(ctx.tableName());
            fieldName = (String) visit(ctx.identifier());
        }
        return new Field(tableName, fieldName);
    }



    @Override
    public Object visitAlphabetIdentifier(SqlParser.AlphabetIdentifierContext ctx) {
        return ctx.ALPHABET_STRING().getText();
    }

    @Override
    public Object visitQuotedIdentifier(SqlParser.QuotedIdentifierContext ctx) {
        String text = ctx.QUOTED_STRING().getText();

        if (text == null) {
            return null;
        }

        text = text.substring(1, text.length() - 1);

        return text;
    }

    @Override
    public Object visitBackQuotedIdentifier(SqlParser.BackQuotedIdentifierContext ctx) {
        String text = ctx.BACKQUOTED_STRING().getText();
        if (text == null) {
            return null;
        }

        text = text.substring(1, text.length() - 1);
        return text;
    }

    @Override
    public Object visitNumIdentifier(SqlParser.NumIdentifierContext ctx) {
        String text = ctx.NUM_STRING().getText();

        if (text == null) {
            return null;
        }
        text = text.substring(1, text.length() - 1);
        return text;
    }

    @Override
    public Object visitVariable(SqlParser.VariableContext ctx) {
        String text = ctx.VARIABLE().getText();
        return text;
    }

    @Override
    public Object visitValues(SqlParser.ValuesContext ctx) {
        List<SqlParser.LiteralContext> literals = ctx.literal();
        List<Object> values = literals.stream().map(this::visit).collect(Collectors.toList());
        return values;
    }

    @Override
    public Object visitNullLiteral(SqlParser.NullLiteralContext ctx) {
        return ctx.NULL().getText();
    }

    @Override
    public Object visitBooleanLiteral(SqlParser.BooleanLiteralContext ctx) {
        String falseBoolean = ctx.FALSE().getText();
        String trueBoolean = ctx.TRUE().getText();

        if (falseBoolean != null) {
            return falseBoolean;
        } else {
            return trueBoolean;
        }
    }

    @Override
    public Object visitNumberLiteral(SqlParser.NumberLiteralContext ctx) {
        return ctx.NUMBER().getText();
    }

    @Override
    public Object visitStringLiteral(SqlParser.StringLiteralContext ctx) {
        String text = ctx.STRING().getText();

        if (text == null) {
            return null;
        }

        text = text.substring(1, text.length() - 1);

        return text;
    }

    @Override
    public Object visitVariableLiteral(SqlParser.VariableLiteralContext ctx) {
        return ctx.VARIABLE().getText();
    }

    @Override
    public Object visitQuotedNumberLiteral(SqlParser.QuotedNumberLiteralContext ctx) {
        String text = ctx.QUOTED_NUMBER().getText();
        if (text == null) {
            return null;
        }

        text = text.substring(1, text.length() - 1);

        return text;
    }

    @Override
    public Object visitQuotedStringLiteral(SqlParser.QuotedStringLiteralContext ctx) {
        String text = ctx.QUOTED_STRING().getText();
        if (text == null) {
            return null;
        }

        text = text.substring(1, text.length() - 1);

        return text;
    }

    private String getIdentifier(SqlParser.IdentifierContext identifier) {
        if (identifier != null) {
            return  (String) visit(identifier);
        }
        return null;
    }


}
