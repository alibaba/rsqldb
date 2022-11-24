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
import com.alibaba.rsqldb.parser.pojo.Column;
import com.alibaba.rsqldb.parser.pojo.FieldType;
import com.alibaba.rsqldb.parser.pojo.Table;
import com.alibaba.rsqldb.parser.util.Pair;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.List;
import java.util.stream.Collectors;

@SuppressWarnings("unchecked")
public class DefaultVisitor extends SqlParserBaseVisitor<Object> {
    @Override
    public Object visitSqlStatements(SqlParser.SqlStatementsContext ctx) {
        Object o = visit(ctx.sqlStatement());

        System.out.println("all over");

        return o;
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
        String columnType = (String) visit(dataTypeContext);

        FieldType type = FieldType.getByType(columnType);
        column.setType(type);


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
    public Object visitQuery(SqlParser.QueryContext ctx) {
        System.out.println("visitQuery");
        return super.visitQuery(ctx);
    }

    @Override
    public Object visitQueryStatement(SqlParser.QueryStatementContext ctx) {
        System.out.println("visitQueryStatement");
        return super.visitQueryStatement(ctx);
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


}
