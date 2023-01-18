/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rsqldb.rest.service.iml;

import com.alibaba.rsqldb.parser.DefaultParser;
import com.alibaba.rsqldb.parser.model.statement.Statement;
import com.alibaba.rsqldb.rest.service.RSQLConfig;
import com.alibaba.rsqldb.rest.service.RSQLConfigBuilder;
import com.alibaba.rsqldb.rest.service.RsqlService;
import com.alibaba.rsqldb.storage.api.Command;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.streams.core.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class DefaultRsqlService implements RsqlService {
    private static final Logger logger = LoggerFactory.getLogger(DefaultRsqlService.class);

    private RSQLConfig rsqlConfig;

    private RSQLEngin rsqlEngin;

    private DefaultParser defaultParser;


    public DefaultRsqlService(RSQLConfigBuilder builder, RSQLEngin rsqlEngin) {
        this.rsqlConfig = builder.build();
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
    public List<String> executeSql(String sql, String jobId) throws Throwable {
        if (StringUtils.isEmpty(sql)) {
            logger.info("sql is null, skip.");
            return null;
        }

        List<Statement> temp = defaultParser.parseStatement(sql);

        ArrayList<String> result = new ArrayList<>();

        for (int i = 0; i < temp.size(); i++) {
            Statement statement = temp.get(i);
            String tempJobId = makeJobId(jobId, statement, i, temp.size());

            this.rsqlEngin.putCommand(tempJobId, statement);

            result.add(tempJobId);
        }

        //todo 需要等待这个流处理任务在本地节点执行成功，才能为CLI交互式查询做准备
        return result;
    }

    private String makeJobId(String jobId, Statement statement, int index, int total) {
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
    public List<Command> queryTask() {
        return this.rsqlEngin.queryAll();
    }

    @Override
    public Command queryTaskByJobId(String jobId) {
        return this.rsqlEngin.queryByJobId(jobId);
    }

    @Override
    public void terminate(String jobId) throws Throwable {
        //终止本地任务
        this.rsqlEngin.terminate(jobId);
    }

    @Override
    public void restart(String jobId) throws Throwable {
        this.rsqlEngin.restart(jobId);
    }

    @Override
    public void remove(String jobId) throws Throwable {
        this.rsqlEngin.remove(jobId);
    }


}
