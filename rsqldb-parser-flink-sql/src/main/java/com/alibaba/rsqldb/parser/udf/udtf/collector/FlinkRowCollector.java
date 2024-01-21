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
package com.alibaba.rsqldb.parser.udf.udtf.collector;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.rsqldb.parser.udf.udtf.FlinkUDTFScript;

import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.model.ThreadContext;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.function.model.FunctionType;

public class FlinkRowCollector implements Collector<Row> {
    private static final String RESULT_TYPE_METHOD_NAME = "getResultType";//获取返回类型的方法名

    protected transient FlinkUDTFScript target;

    public FlinkRowCollector(FlinkUDTFScript target) {
        this.target = target;
    }

    @Override
    public void collect(Row row) {
        FunctionContext context = (FunctionContext)loadContext();
        int size = row.getArity();
        IMessage message = context.getMessage();
        final JSONObject jsonObject = message.getMessageBody();
        JSONObject newMessage = new JSONObject();
        newMessage.putAll(jsonObject);
        for (int i = 0; i < size; i++) {
            if (row.getField(i) == null) {
                continue;
            }
            newMessage.put(FunctionType.UDTF.getName() + i, row.getField(i));
        }
        Message msg = new Message(newMessage);
        msg.setHeader(message.getHeader().copy());
        context.addSplitMessages(msg);
        context.openSplitModel();
    }

    private AbstractContext loadContext() {
        ThreadContext threadContext = ThreadContext.getInstance();
        return threadContext.get();
    }

    @Override
    public void close() {

    }
}
