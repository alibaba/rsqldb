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
package com.alibaba.rsqldb.parser.builder;

import java.util.HashSet;
import java.util.Set;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.rsqldb.parser.udf.FunctionScriptUDAF;
import com.alibaba.rsqldb.parser.udf.function.AccumulatorFunction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.compiler.CustomJavaCompiler;
import org.apache.rocketmq.streams.common.model.NameCreatorContext;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.script.ScriptComponent;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.service.udf.UDFScript;

/**
 * UDF's SQL Builder
 */
public class FunctionSqlBuilder extends AbstractSqlBuilder {
    private static final Log LOG = LogFactory.getLog(FunctionSqlBuilder.class);

    protected String functionName;
    protected String className;

    @Override
    public void build() {
        BlinkUDFScan blinkUDFScan = new BlinkUDFScan(getConfiguration());
        if (Function.class.getName().equalsIgnoreCase(className)) {
            return;
        }
        if (isJavaSourceCode(className)) {
            CustomJavaCompiler compiler = new CustomJavaCompiler(className);
            Class<?> o = compiler.compileClass();
            blinkUDFScan.doProcessor(o, functionName);
            className = o.getName();

        } else if (isUDAFCode(className)) {
            FunctionScriptUDAF functionScriptUDAF = new FunctionScriptUDAF();
            blinkUDFScan.className2Scripts.put(FunctionScriptUDAF.class.getName(), functionScriptUDAF);
            functionScriptUDAF.setFullClassName(FunctionScriptUDAF.class.getName());
            functionScriptUDAF.setFunctionName(functionName);
            JSONObject msg = new JSONObject();
            msg.put(AccumulatorFunction.ACCUMULATOR_UDF_KEY, functionScriptUDAF);
            ScriptComponent.getInstance().getFunctionService().scanPackage("com.alibaba.rsqldb.parser.udf.function");
            ScriptComponent.getInstance().getService().executeScript(msg, className);
            this.className = FunctionScriptUDAF.class.getName();
        }
        UDFScript blinkUDFScript = blinkUDFScan.getScript(className, functionName);
        if (blinkUDFScript == null) {
            blinkUDFScan.scanClass(className, functionName);
            blinkUDFScript = blinkUDFScan.getScript(className, functionName);
            if (blinkUDFScript == null) {
                blinkUDFScript = blinkUDFScan.getScript(className, null);
            }
            if (blinkUDFScript == null) {
                throw new RuntimeException("Can_Not_Found_Udf(" + className + ")");
            }
        }
        blinkUDFScript.setFunctionName(functionName);
        blinkUDFScript.setNameSpace(getPipelineBuilder().getPipelineNameSpace());
        String name = MapKeyUtil.createKey(getPipelineBuilder().getPipelineName(), NameCreatorContext.get().createName(functionName));
        blinkUDFScript.setName(name);
        getPipelineBuilder().addConfigurables(blinkUDFScript);
        getPipelineBuilder().getPipeline().getUdfs().add(blinkUDFScript);
    }

    private boolean isUDAFCode(String className) {
        if (className == null) {
            return false;
        }
        className = className.trim();
        return className.startsWith("accumulator");
    }

    protected boolean isJavaSourceCode(String className) {
        if (className == null) {
            return false;
        }
        className = className.trim();
        return className.startsWith("package") || className.startsWith("import") || className.startsWith("public") || className.startsWith("class") || className.endsWith("}");
    }

    @Override
    public String getFieldName(String fieldName, boolean containsSelf) {
        return null;
    }

    @Override
    public Set<String> parseDependentTables() {
        return new HashSet<>();
    }

    public String getFunctionName() {
        return functionName;
    }

    public void setFunctionName(String functionName) {
        this.functionName = functionName;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

}
