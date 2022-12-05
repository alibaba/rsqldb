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
package com.alibaba.rsqldb.parser;


import com.alibaba.rsqldb.parser.exception.SyntaxErrorException;
import com.alibaba.rsqldb.parser.impl.DefaultErrorListener;
import com.alibaba.rsqldb.parser.impl.DefaultVisitor;
import com.alibaba.rsqldb.parser.model.Node;
import com.alibaba.rsqldb.parser.util.ParserUtil;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.Interval;
import org.apache.commons.lang3.StringUtils;
import com.alibaba.rsqldb.parser.SqlParser;
import com.alibaba.rsqldb.parser.SqlLexer;


public class DefaultParser implements RsqlParser {
    @Override
    public void parse(String sql) throws SyntaxErrorException {
        if (StringUtils.isEmpty(sql)) {
            return;
        }

        CodePointCharStream charStream = CharStreams.fromString(sql);
        SqlLexer sqlLexer = new SqlLexer(charStream);
        CommonTokenStream tokens = new CommonTokenStream(sqlLexer);
        SqlParser parser = new SqlParser(tokens);

        sqlLexer.addErrorListener(new DefaultErrorListener());
        parser.addErrorListener(new DefaultErrorListener());


//        parser.getInterpreter().setPredictionMode(PredictionMode.SLL);

        SqlParser.SqlStatementsContext statements = parser.sqlStatements();

        DefaultVisitor visitor = new DefaultVisitor();
        Node result = visitor.visit(statements);

//        String result = ParserUtil.getText(statements);

        System.out.println(result);
    }
}
