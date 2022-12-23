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
package com.alibaba.rsqldb.rest.service.iml;

import com.alibaba.rsqldb.parser.DefaultParser;
import com.alibaba.rsqldb.parser.model.Node;
import com.alibaba.rsqldb.parser.model.statement.Statement;
import com.alibaba.rsqldb.rest.service.RSQLConfig;
import com.alibaba.rsqldb.rest.service.RsqlService;
import com.alibaba.rsqldb.rest.store.CommandQueue;
import com.alibaba.rsqldb.rest.store.CommandStore;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

@Service
public class DefaultRsqlService implements RsqlService {
    private static final Logger logger = LoggerFactory.getLogger(DefaultRsqlService.class);

    @Resource
    private RSQLConfig rsqlConfig;
    private DefaultParser defaultParser;
    private CommandQueue commandQueue;

    public DefaultRsqlService() {
        this.defaultParser = new DefaultParser();
        this.commandQueue = new CommandStore(rsqlConfig);
        this.commandQueue.start();
    }

    /**
     * 解析sql
     * 将sql持久化
     * 获取一个sql
     * 提交给线程池执行
     * @param sql
     */
    @Override
    public void executeSql(String sql) {
        if (StringUtils.isEmpty(sql)) {
            return;
        }
        //解析
        List<Statement> temp = defaultParser.parseStatement(sql);

        for (Statement statement : temp) {
            //写入RocketMQ,放入后保证是能执行的，不然不要放入
            this.commandQueue.putCommand(statement);
        }


        //异步执行



        //返回执行结果


    }
    //执行流处理任务


    //执行属性设置


    //执行select语句并返回结果，在目录上实时显示

}
