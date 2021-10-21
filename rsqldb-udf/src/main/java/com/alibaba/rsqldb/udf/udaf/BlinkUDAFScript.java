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
package com.alibaba.rsqldb.udf.udaf;

import org.apache.rocketmq.streams.script.service.udf.UDAFScript;

public class BlinkUDAFScript extends UDAFScript {

    public BlinkUDAFScript() {
        this.accumulateMethodName = "accumulate";
        this.createAccumulatorMethodName = "createAccumulator";
        this.getValueMethodName = "getValue";
        this.retractMethodName = "retract";
        this.mergeMethodName = "merge";
        this.methodName = "eval";
        this.initMethodName = "open";
    }

    @Override
    protected Object[] convert(String... parameters) {
        if (parameters == null) {
            return new Object[0];
        }
        Object[] datas = new Object[parameters.length];
        //第一个参数是内置的
        for (int i = 0; i < parameters.length; i++) {
            datas[i] = accumulateFunctionConfigure.getParameterDataTypes()[i + 1].getData(parameters[i]);
        }
        return datas;
    }

    @Override
    protected Object createMergeParamters(Iterable its) {
        return its;
    }

}
