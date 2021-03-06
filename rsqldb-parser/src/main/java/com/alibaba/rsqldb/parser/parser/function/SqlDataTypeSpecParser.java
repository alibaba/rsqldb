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
package com.alibaba.rsqldb.parser.parser.function;

import com.alibaba.rsqldb.parser.parser.AbstractSqlParser;
import com.alibaba.rsqldb.parser.parser.builder.AbstractSQLBuilder;
import com.alibaba.rsqldb.parser.parser.result.ConstantParseResult;
import com.alibaba.rsqldb.parser.parser.result.IParseResult;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.rocketmq.streams.common.datatype.StringDataType;

public class SqlDataTypeSpecParser extends AbstractSqlParser<SqlDataTypeSpec, AbstractSQLBuilder> {

    @Override
    public IParseResult parse(AbstractSQLBuilder tableDescriptor, SqlDataTypeSpec sqlDataTypeSpec) {
        return new ConstantParseResult(sqlDataTypeSpec.toString(), new StringDataType());
    }

    @Override
    public boolean support(Object sqlNode) {
        return sqlNode instanceof SqlDataTypeSpec;
    }
}
