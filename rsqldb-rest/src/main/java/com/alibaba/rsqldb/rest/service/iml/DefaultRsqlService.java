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
import com.alibaba.rsqldb.parser.model.Calculator;
import com.alibaba.rsqldb.parser.model.Field;
import com.alibaba.rsqldb.parser.model.statement.Statement;
import com.alibaba.rsqldb.parser.model.statement.query.QueryStatement;
import com.alibaba.rsqldb.rest.response.QueryResult;
import com.alibaba.rsqldb.rest.service.RSQLConfig;
import com.alibaba.rsqldb.rest.service.RsqlService;
import com.alibaba.rsqldb.rest.store.CommandResult;
import com.alibaba.rsqldb.rest.store.CommandStatus;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.streams.core.common.Constant;
import org.apache.rocketmq.streams.core.util.Pair;
import org.apache.rocketmq.streams.core.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class DefaultRsqlService implements RsqlService {
    private static final Logger logger = LoggerFactory.getLogger(DefaultRsqlService.class);

    private RSQLConfig rsqlConfig;

    private RSQLEngin rsqlEngin;

    private DefaultParser defaultParser;


    public DefaultRsqlService(RSQLConfig rsqlConfig, RSQLEngin rsqlEngin) {
        this.rsqlConfig = rsqlConfig;
        this.rsqlEngin = rsqlEngin;
        this.defaultParser = new DefaultParser();
    }

    /**
     * 解析sql
     * 将sql持久化
     * 获取一个sql
     * 提交给线程池执行
     *
     * @param sql
     */
    @Override
    public List<String> executeSql(String sql, String jobId) {
        if (StringUtils.isEmpty(sql)) {
            logger.info("sql is null, skip.");
            return null;
        }

        List<Statement> temp = defaultParser.parseStatement(sql);

        ArrayList<String> result = new ArrayList<>();

        for (int i = 0; i < temp.size(); i++) {
            Statement statement = temp.get(i);
            String tempJobId = makeJobId(jobId, statement, i, temp.size());

            this.rsqlEngin.putStatement(tempJobId, statement);

            result.add(tempJobId);
        }

        //todo 需要等待这个流处理任务在本地节点执行成功，才能为CLI交互式查询做准备
        return result;
    }

    private static String makeJobId(String jobId, Statement statement, int index, int total) {
        if (!StringUtils.isEmpty(jobId) && total == 1) {
            return jobId;
        }

        String tempJobId;

        if (StringUtils.isEmpty(jobId)) {
            tempJobId = Utils.toHexString(statement.getContent());
        } else {
            tempJobId = jobId;
        }

        if (!StringUtils.isEmpty(jobId)) {
            tempJobId = String.join("@", tempJobId, String.valueOf(index));
        }

        logger.info("create jobId from sql. jobId=[{}], sql=[{}]", tempJobId, statement.getContent());

        return tempJobId;
    }

    @Override
    public List<QueryResult> queryTask() {
        return this.rsqlEngin.queryAll();
    }

    @Override
    public QueryResult queryTaskByJobId(String jobId) {
        return this.rsqlEngin.queryByJobId(jobId);
    }

    @Override
    public void terminate(String jobId) {
        //终止本地任务
        this.rsqlEngin.terminate(jobId);
    }

    @Override
    public void restart(String jobId) {
        this.rsqlEngin.restart(jobId);
    }

    @Override
    public void remove(String jobId) {
        this.rsqlEngin.remove(jobId);
    }


}
