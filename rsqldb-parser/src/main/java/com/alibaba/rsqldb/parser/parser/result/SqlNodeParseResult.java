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
package com.alibaba.rsqldb.parser.parser.result;

import com.alibaba.rsqldb.parser.parser.ISqlParser;
import com.alibaba.rsqldb.parser.parser.SQLNodeParserFactory;
import com.alibaba.rsqldb.parser.parser.builder.SelectSQLBuilder;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.calcite.sql.SqlNode;

/**
 * 每个解析的返回结果。需要考虑变量，常量和脚本的情况
 */
public class SqlNodeParseResult extends ScriptParseResult {

    /**
     * sqlnode
     */
    protected SqlNode sqlNode;

    /**
     * 解析结果
     */
    protected ScriptParseResult scriptParseResult;

    /**
     * 是否已经解析过了
     */
    protected AtomicBoolean isParse = new AtomicBoolean(false);

    /**
     * 在脚本执行中产生的新依赖
     */
    protected Set<String> fieldDependents = new HashSet<>();

    public SqlNodeParseResult(SqlNode sqlNode) {
        this.sqlNode = sqlNode;
    }

    /**
     * 解析select的as sqlnode
     */
    protected void parse() {
        if (!isParse.compareAndSet(false, true)) {
            return;
        }
        SelectSQLBuilder selectTableDesciptor = new SelectSQLBuilder();
        //selectTableDesciptor.setOpenScriptDependent(true);
        ISqlParser sqlParser = SQLNodeParserFactory.getParse(sqlNode);
        if (NotSupportParseResult.class.isInstance(sqlParser)) {
            NotSupportParseResult notSupportParseResult = (NotSupportParseResult) sqlParser;
            throw new RuntimeException("can not support this parser " + notSupportParseResult.getSqlNode().toString());
        }
        IParseResult sqlVar = sqlParser.parse(selectTableDesciptor, sqlNode);
        if (NotSupportParseResult.class.isInstance(sqlVar)) {
            NotSupportParseResult notSupportParseResult = (NotSupportParseResult) sqlVar;
            throw new RuntimeException("can not support this parser " + notSupportParseResult.getSqlNode().toString());
        }
        if (ScriptParseResult.class.isInstance(sqlVar)) {
            ScriptParseResult scriptParseResult = (ScriptParseResult) sqlVar;
            if (scriptParseResult.getReturnValue() != null) {
                List<String> scripts = selectTableDesciptor.getScripts();
                if (scripts == null) {
                    scripts = new ArrayList<>();
                    scripts.addAll(scriptParseResult.getScriptValueList());
                }
                scriptParseResult.setScriptValueList(scripts);
                this.scriptParseResult = scriptParseResult;
            }
        }
        //fieldDependents.addAll(selectTableDesciptor.getScriptDependentFieldNames());
    }

    @Override
    public String getValueForSubExpression() {
        parse();
        if (this.scriptParseResult != null) {
            return scriptParseResult.getValueForSubExpression();
        }
        return null;
    }

    @Override
    public String getReturnValue() {
        parse();
        if (this.scriptParseResult != null) {
            return scriptParseResult.getReturnValue();
        }
        return null;
    }

    @Override
    public Object getResultValue() {
        parse();
        if (this.scriptParseResult != null) {
            return scriptParseResult.getResultValue();
        }
        return null;
    }

    @Override
    public boolean isConstantString() {
        parse();
        if (this.scriptParseResult != null) {
            return scriptParseResult.isConstantString();
        }
        return false;
    }

    @Override
    public List<String> getScriptValueList() {
        parse();
        if (this.scriptParseResult != null) {
            return scriptParseResult.getScriptValueList();
        }
        return null;
    }

    @Override
    public void addScript(String script) {
        throw new RuntimeException("can not support this method");
    }

    @Override
    public void addScript(SelectSQLBuilder tableDescriptor, String script) {
        throw new RuntimeException("can not support this method");
    }

    @Override
    public String getScript() {
        parse();
        if (this.scriptParseResult != null) {
            return scriptParseResult.getScript();
        }
        return null;
    }

    @Override
    public void setScriptValueList(List<String> scriptValueList) {
        throw new RuntimeException("can not support this method");
    }

    public Set<String> getFieldDependents() {
        return fieldDependents;
    }

    @Override
    public void setReturnValue(String returnValue) {
        throw new RuntimeException("can not support this method");
    }
}
