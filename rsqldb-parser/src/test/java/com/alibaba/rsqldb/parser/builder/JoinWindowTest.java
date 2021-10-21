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

import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.streams.common.channel.source.systemmsg.NewSplitMessage;
import org.apache.rocketmq.streams.common.channel.source.systemmsg.RemoveSplitMessage;
import org.apache.rocketmq.streams.common.checkpoint.CheckPointMessage;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.Context;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.topology.ChainStage;
import org.apache.rocketmq.streams.common.topology.model.IStageHandle;
import org.apache.rocketmq.streams.window.operator.join.JoinWindow;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class JoinWindowTest {
    private static final String WINDOW_NAMESPACE = "chris_tmp_10";
    private static final String WINDOW_NAME = "tmp10";

    @Test
    public void testJoinWindow() throws InterruptedException {
        Map<String, List<IMessage>> result = new ConcurrentHashMap<>();
        JoinWindow joinWindow = createJoinWindow();

        joinWindow.setFireReceiver(new Receiver(new ChainStage() {
            @Override
            public void checkpoint(IMessage message, AbstractContext context, CheckPointMessage checkPointMessage) {

            }

            @Override
            public void addNewSplit(IMessage message, AbstractContext context, NewSplitMessage newSplitMessage) {

            }

            @Override
            public void removeSplit(IMessage message, AbstractContext context, RemoveSplitMessage removeSplitMessage) {

            }

            @Override
            public boolean isAsyncNode() {
                return false;
            }

            @Override
            protected IStageHandle selectHandle(IMessage iMessage, AbstractContext context) {
                return null;
            }
        }));
        for (int i = 0; i < 1; i++) {
            batchAddMsg(joinWindow);
            //            joinWindow.run();
            Thread.sleep(150000);
        }

        for (Map.Entry<String, List<IMessage>> tmp : result.entrySet()) {
            for (IMessage msg : tmp.getValue()) {
                System.out.println("joinwindow==" + tmp.getKey() + "==" + msg.getMessageBody().toJSONString());
            }
        }

        Thread.sleep(100000000);
    }

    private JoinWindow createJoinWindow() {

        JoinWindow window = new JoinWindow();
        window.setNameSpace(WINDOW_NAMESPACE);
        window.setConfigureName(WINDOW_NAME);
        window.setSlideInterval(5);
        //        window.setFireDelaySecond(5);
        window.setSizeInterval(5);
        window.setWaterMarkMinute(1);
        window.setJoinType("INNER");
        List<String> fields = new ArrayList<>();
        fields.add("name");
        window.setLeftJoinFieldNames(fields);
        window.setRightJoinFieldNames(fields);
        window.setExpression(null);
        window.setRightAsName("a");
        window.setRetainWindowCount(4);

        window.toObject(window.toJson());
        window.init();
        return window;
    }

    /**
     * 插入测试数据
     *
     * @param window
     */
    protected void batchAddMsg(JoinWindow window) {
        AtomicInteger count = new AtomicInteger(100);
        for (int i = 0; i < 10; i++) {
            JSONObject msg1 = new JSONObject();
            msg1.put("name", "chris" + i % 5);
            msg1.put("age", 18);
            msg1.put("time", System.currentTimeMillis());
            msg1.put("numid", count.incrementAndGet());
            Message message1 = new Message(msg1);
            message1.getHeader().setQueueId(String.valueOf(System.currentTimeMillis()));
            message1.getHeader().setOffset(i + "left");
            message1.getHeader().setMsgRouteFromLable("left");
            Context context1 = new Context(message1);
            window.doMessage(message1, context1);

            JSONObject msg = new JSONObject();
            msg.put("name", "chris" + i % 5);
            msg.put("age", 20);
            msg.put("time", System.currentTimeMillis());
            msg.put("numid", count.incrementAndGet());
            Message message = new Message(msg);
            message.getHeader().setQueueId(String.valueOf(System.currentTimeMillis()));
            message.getHeader().setOffset(i + "right");
            message.getHeader().setMsgRouteFromLable("right");
            Context context = new Context(message);
            window.doMessage(message, context);
        }

        //        for(int i=0;i<10;i++){
        //            JSONObject msg=new JSONObject();
        //            msg.put("name","chris" + i%5);
        //            msg.put("age",20);
        //            msg.put("time", System.currentTimeMillis());
        //            msg.put("numid", count.incrementAndGet());
        //            Message message=new Message(msg);
        //            message.getHeader().setQueueId(String.valueOf(System.currentTimeMillis()));
        //            message.getHeader().setOffset(i+"right");
        //            message.getHeader().setMsgRouteFromLable("right");
        //            Context context=new Context(message);
        //            window.doMessage(message,context);
        //        }
        //        window.flush();
    }

    public class Receiver extends ChainStage.PiplineRecieverAfterCurrentNode {

        public Receiver(ChainStage stage) {
            stage.super();
        }

        @Override
        public AbstractContext<IMessage> doMessage(IMessage message, AbstractContext context) {
            System.out.println("************" + message.getMessageBody().toJSONString());
            return null;
        }

    }
}
