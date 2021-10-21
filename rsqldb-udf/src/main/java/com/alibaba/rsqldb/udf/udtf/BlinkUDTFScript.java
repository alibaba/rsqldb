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
package com.alibaba.rsqldb.udf.udtf;

import com.alibaba.rsqldb.udf.udtf.collector.BlinkCollector;
import org.apache.rocketmq.streams.script.service.udf.UDTFScript;
import org.apache.flink.util.Collector;

public class BlinkUDTFScript extends UDTFScript {

    public BlinkUDTFScript() {
        this.methodName = "eval";
        this.initMethodName = "open";
        this.setSetCollectorMethodName("setCollector");
    }

    @Override
    protected Class getCollectorClasss() {
        return Collector.class;
    }

    @Override
    protected Object getCollector() {
        return new BlinkCollector(this);
    }

}
