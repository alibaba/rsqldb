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
package com.alibaba.rsqldb.udf.udtf.collector;

import com.alibaba.fastjson.JSONObject;

import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.common.model.ThreadContext;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.util.Collector;
import org.apache.rocketmq.streams.script.function.model.FunctionType;

public class BlinkTupleCollector implements Collector<Tuple> {

    @Override
    public void collect(Tuple tuple) {
        FunctionContext context = loadContext();
        IMessage message = context.getMessage();
        int size = tuple.getArity();
        final JSONObject jsonObject = message.getMessageBody();
        JSONObject newMessage = new JSONObject();
        newMessage.putAll(jsonObject);
        for (int i = 0; i < size; i++) {
            newMessage.put(FunctionType.UDTF.getName() + i, tuple.getField(i));
        }
        Message msg = new Message(newMessage);
        msg.setHeader(message.getHeader().copy());
        ;
        context.addSplitMessages(msg);
        context.openSplitModel();
    }

    private FunctionContext loadContext() {
        ThreadContext threadContext = ThreadContext.getInstance();
        return (FunctionContext)threadContext.get();
    }

    @Override
    public void close() {

    }
}
