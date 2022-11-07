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
package com.alibaba.rsqldb.parser.udf.udf;

import java.io.File;
import org.apache.flink.api.common.accumulators.DoubleCounter;
import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.script.service.udf.UDFScript;

public class BlinkUDFScript extends UDFScript {

    protected transient FunctionContext functionContext = new FunctionContext() {

        @Override
        public MetricGroup getMetricGroup() {
            return null;
        }

        @Override
        public File getCachedFile(String name) {
            return null;
        }

        @Override
        public int getNumberOfParallelSubtasks() {
            return 0;
        }

        @Override
        public int getIndexOfThisSubtask() {
            return 0;
        }

        @Override
        public IntCounter getIntCounter(String name) {
            return null;
        }

        @Override
        public LongCounter getLongCounter(String name) {
            return null;
        }

        @Override
        public DoubleCounter getDoubleCounter(String name) {
            return null;
        }

        @Override
        public Histogram getHistogram(String name) {
            return null;
        }

        @Override
        public String getJobParameter(String key, String defaultValue) {
           return ComponentCreator.getProperties().getProperty(key,defaultValue);
        }
    };

    public BlinkUDFScript() {
        this.methodName = "eval";
        this.initMethodName = "open";
        initParameters = new Object[] {functionContext};
    }

}
