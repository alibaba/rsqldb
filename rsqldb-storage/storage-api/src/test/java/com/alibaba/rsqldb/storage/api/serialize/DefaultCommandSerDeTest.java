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
package com.alibaba.rsqldb.storage.api.serialize;

import com.alibaba.rsqldb.parser.DefaultParser;
import com.alibaba.rsqldb.parser.model.Node;
import com.alibaba.rsqldb.parser.model.Operator;
import com.alibaba.rsqldb.parser.model.baseType.NumberType;
import com.alibaba.rsqldb.parser.model.expression.Expression;
import com.alibaba.rsqldb.parser.model.expression.SingleValueExpression;
import com.alibaba.rsqldb.parser.model.statement.Statement;
import com.alibaba.rsqldb.parser.model.statement.query.FilterQueryStatement;
import com.alibaba.rsqldb.storage.api.Command;
import com.alibaba.rsqldb.storage.api.CommandStatus;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class DefaultCommandSerDeTest {
    private DefaultCommandSerDe commandSerDe = new DefaultCommandSerDe();

    @Test
    public void test0() throws Throwable {
        String sql = "select field_1\n" +
                "     , field_2\n" +
                "     , field_3\n" +
                "     , field_4\n" +
                "from rocketmq_source where field_1=23;";

        DefaultParser parser = new DefaultParser();
        List<Statement> statements = parser.parseStatement(sql);

        assertEquals(1, statements.size());

        Statement statement = statements.get(0);

        Command command = new Command("1212", statement, CommandStatus.RUNNING);

        byte[] serialize = commandSerDe.serialize(command);

        Command target = commandSerDe.deserialize(serialize);

        assertEquals("1212", target.getJobId());
        assertSame(CommandStatus.RUNNING, target.getStatus());

        Node node = target.getNode();
        assertTrue(node instanceof FilterQueryStatement);

        FilterQueryStatement filterQueryStatement = (FilterQueryStatement) node;
        assertEquals("rocketmq_source", filterQueryStatement.getTableName());
        assertEquals(4, filterQueryStatement.getSelectFieldAndCalculator().size());

        Expression filter = filterQueryStatement.getFilter();
        assertTrue(filter instanceof SingleValueExpression);

        SingleValueExpression expression = (SingleValueExpression) filter;

        assertEquals(23, ((NumberType)expression.getValue()).getNumber());
        assertEquals("field_1", expression.getField().getFieldName());
        assertSame(Operator.EQUAL, expression.getOperator());
    }
}
