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
package com.alibaba.rsqldb.parser.function;

import com.alibaba.rsqldb.parser.AbstractSqlNodeParser;
import com.alibaba.rsqldb.parser.builder.AbstractSqlBuilder;
import com.alibaba.rsqldb.parser.creator.ParserNameCreator;
import com.alibaba.rsqldb.parser.result.IParseResult;
import com.alibaba.rsqldb.parser.result.ScriptParseResult;
import com.alibaba.rsqldb.parser.result.VarParseResult;

import org.apache.calcite.sql.SqlIdentifier;

public class SQLNodeIndentifierParser extends AbstractSqlNodeParser<SqlIdentifier, AbstractSqlBuilder> {

    @Override
    public IParseResult parse(AbstractSqlBuilder builder, SqlIdentifier sqlIdentifier) {
        /**
         * 把where中用到的所有字段保存下来
         */
        String fieldName = sqlIdentifier.toString();
        if ("CURRENT_TIMESTAMP".equals(fieldName)) {
            ScriptParseResult scriptParseResult = new ScriptParseResult();
            String name = ParserNameCreator.createName("now");
            scriptParseResult.addScript(name + "=now();");
            scriptParseResult.setReturnValue(name);
            return scriptParseResult;
        }
        //        if(SelectSQLBuilder.class.isInstance(builder)){
        //            SelectSQLBuilder selectSQLDescriptor=(SelectSQLBuilder)builder;
        //            if(selectSQLDescriptor.isWhereStage()){
        //                //selectSQLDescriptor.addDependentField(fieldName);
        //            }
        ////            if(selectSQLDescriptor.isOpenScriptDependent()){
        ////                selectSQLDescriptor.getScriptDependentFieldNames().add(fieldName);
        ////            }
        //        }
        return new VarParseResult(builder.getFieldName(fieldName));
    }

    @Override
    public boolean support(Object sqlNode) {
        return sqlNode instanceof SqlIdentifier;
    }

}
