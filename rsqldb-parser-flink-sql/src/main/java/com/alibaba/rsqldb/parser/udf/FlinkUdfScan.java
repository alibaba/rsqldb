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
package com.alibaba.rsqldb.parser.udf;

import com.alibaba.rsqldb.parser.udf.udaf.FlinkUDAFScript;
import com.alibaba.rsqldb.parser.udf.udf.FlinkUDFScript;
import com.alibaba.rsqldb.parser.udf.udtf.FlinkUDTFScript;

import com.google.auto.service.AutoService;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.rocketmq.streams.common.model.ServiceName;
import org.apache.rocketmq.streams.script.service.IUDFScan;
import org.apache.rocketmq.streams.script.service.udf.UDFScript;

@AutoService(IUDFScan.class)
@ServiceName("flink")
public class FlinkUdfScan implements IUDFScan {
    @Override
    public UDFScript create(Class clazz, String functionName) {
        UDFScript script = null;
        if (TableFunction.class.isAssignableFrom(clazz)) {
            script = new FlinkUDTFScript();
        } else if (AggregateFunction.class.isAssignableFrom(clazz)) {
            script = new FlinkUDAFScript();
        } else {
            script = new FlinkUDFScript();
        }
        return script;
    }

    @Override
    public boolean isSupport(Class clazz) {
        if (ScalarFunction.class.isAssignableFrom(clazz) || TableFunction.class.isAssignableFrom(clazz) || AggregateFunction.class.isAssignableFrom(clazz)) {
            return true;
        }
        return false;
    }
}
