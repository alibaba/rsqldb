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
package com.alibaba.rsqldb.parser;


import com.alibaba.rsqldb.common.exception.SyntaxErrorException;
import com.alibaba.rsqldb.parser.impl.DefaultErrorListener;
import com.alibaba.rsqldb.parser.impl.DefaultVisitor;
import com.alibaba.rsqldb.parser.model.ListNode;
import com.alibaba.rsqldb.parser.model.Node;
import com.alibaba.rsqldb.parser.model.statement.Statement;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class DefaultParser implements RsqlParser {
    private final DefaultVisitor visitor = new DefaultVisitor();

    @Override
    @SuppressWarnings("unchecked")
    public List<Statement> parseStatement(String sql) throws SyntaxErrorException {
        List<Statement> result = new ArrayList<>();
        if (StringUtils.isEmpty(sql)) {
            return result;
        }

        CodePointCharStream charStream = CharStreams.fromString(sql);
        com.alibaba.rsqldb.parser.SqlLexer sqlLexer = new com.alibaba.rsqldb.parser.SqlLexer(charStream);
        CommonTokenStream tokens = new CommonTokenStream(sqlLexer);
        com.alibaba.rsqldb.parser.SqlParser parser = new com.alibaba.rsqldb.parser.SqlParser(tokens);

        sqlLexer.removeErrorListeners();
        sqlLexer.addErrorListener(new DefaultErrorListener());

        parser.removeErrorListeners();
        parser.addErrorListener(new DefaultErrorListener());

        com.alibaba.rsqldb.parser.SqlParser.SqlStatementsContext statements = parser.sqlStatements();


        ListNode<Node> nodes = (ListNode<Node>) visitor.visit(statements);

        for (Node node : nodes.getHolder()) {
            if (node instanceof Statement) {
                result.add((Statement) node);
            } else {
                throw new SyntaxErrorException("not a statement sql.");
            }
        }

        return result;
    }
}
