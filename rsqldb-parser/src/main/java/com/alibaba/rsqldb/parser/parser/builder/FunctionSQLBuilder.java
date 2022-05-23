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
package com.alibaba.rsqldb.parser.parser.builder;

import java.util.HashSet;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.model.NameCreatorContext;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.service.udf.UDFScript;

/**
 * UDX's SQL Builder
 */
public class FunctionSQLBuilder extends AbstractSQLBuilder<AbstractSQLBuilder> {
    private static final Log LOG = LogFactory.getLog(FunctionSQLBuilder.class);
    protected BlinkUDFScan blinkUDFScan = BlinkUDFScan.getInstance();

    protected String functionName;

    protected String className;

    @Override
    public void build() {
        if (Function.class.getName().equalsIgnoreCase(className)) {
            return;
        }
        UDFScript blinkUDFScript = blinkUDFScan.getScript(className, functionName);
        if (blinkUDFScript == null) {
            blinkUDFScan.scan(className, null, functionName);
            blinkUDFScript = blinkUDFScan.getScript(className, functionName);
            if (blinkUDFScript == null) {
                blinkUDFScript = blinkUDFScan.getScript(className, null);
            }
            if (blinkUDFScript == null ) {
                LOG.error("can not find udf, the udf is " + className);
                return;
            }
        }
        blinkUDFScript.setFunctionName(functionName);
        //        blinkUDFScan.scan(null);
//        blinkUDFScript.setFunctionName(functionName);
        blinkUDFScript.setNameSpace(getPipelineBuilder().getPipelineNameSpace());
        String name = MapKeyUtil.createKey(getPipelineBuilder().getPipelineName(), NameCreatorContext.get().createNewName(functionName));
        blinkUDFScript.setConfigureName(name);
        getPipelineBuilder().addConfigurables(blinkUDFScript);
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
