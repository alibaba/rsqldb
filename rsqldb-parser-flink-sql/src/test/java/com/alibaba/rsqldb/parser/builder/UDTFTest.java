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
import com.alibaba.rsqldb.parser.udf.udtf.FlinkUDTFScript;
import java.util.List;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.script.ScriptComponent;
import org.junit.Test;

public class UDTFTest {
    @Test
    public void testUDTF(){
        FlinkUDTFScript udtfScript=new FlinkUDTFScript();
        udtfScript.setFullClassName(UDTFFunction.class.getName());
        udtfScript.setFunctionName("abc");
        udtfScript.init();
        JSONObject msg=new JSONObject();
        msg.put("name","chris");
       List<IMessage>  messageList=ScriptComponent.getInstance().getService().executeScript(msg,"abc()");
       for(IMessage message:messageList){
           System.out.println(message.getMessageBody());
       }
    }
}
