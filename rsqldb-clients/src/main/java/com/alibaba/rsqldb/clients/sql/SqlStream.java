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
package com.alibaba.rsqldb.clients.sql;

import com.alibaba.rsqldb.parser.parser.builder.BlinkUDFScan;
import com.alibaba.rsqldb.parser.sql.ISqlParser;
import java.io.File;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.flink.util.FileUtils;
import org.apache.rocketmq.streams.client.strategy.Strategy;
import org.apache.rocketmq.streams.common.channel.impl.view.ViewSource;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.tasks.StreamTask;

/**
 * can execute sql directly can submit sql to server support sql assemble
 */
public class SqlStream extends AbstractStream<SqlStream> {

    protected String taskName;

    protected AtomicBoolean isStart = new AtomicBoolean(false);

    protected StreamTask streamsTask;

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public static SqlStream create(String namespace) {
        return new SqlStream(namespace);
    }

    private SqlStream(String namespace) {
        super(namespace);
    }

    public SqlStream with(Strategy... strategies) {
        Properties properties = new Properties();
        for (Strategy strategy : strategies) {
            properties.putAll(strategy.getStrategyProperties());
        }
        this.properties.putAll(properties);
        return this;
    }

    public SqlStream register(String jarDir, String... packageNames) {
        if (packageNames != null) {
            for (String packageName : packageNames) {
                BlinkUDFScan.getInstance().scan(jarDir, packageName);
            }
        }
        return this;
    }

    public SqlStream name(String taskName) throws Exception {
        if (this.configurableComponent == null) {
            throw new Exception("you must execute init before execute name function ");
        }
        this.taskName = taskName;
        return this;
    }

    public SqlStream sqlPath(String sqlPath) throws Exception {
        String sql = FileUtils.readFile(new File(sqlPath), "utf-8");
        return sql(sql);
    }

    public SqlStream sql(String sql) throws Exception {
        if (this.configurableComponent == null) {
            throw new Exception("you must execute init before execute sql function");
        }
        if (this.taskName == null) {
            throw new Exception("you must execute name before execute sql function ");
        }
        ISqlParser iSqlParser = createSqlTreeBuilder(namespace, taskName, sql, this.configurableComponent).parse(false);
        List<ChainPipeline<?>> pipelines = iSqlParser.pipelines();
        List<ChainPipeline<?>> pipelinesForStreamTask = pipelines.stream().filter(pipeline -> pipeline.getSource() != null && !(pipeline.getSource() instanceof ViewSource)).collect(Collectors.toList());
        this.streamsTask = new StreamTask();
        this.streamsTask.setPipelines(pipelinesForStreamTask);
        this.streamsTask.setConfigureName(this.taskName);
        this.streamsTask.setNameSpace(this.namespace);
        return this;
    }

    @Override public List<StreamTask> list() throws Exception {
        if (this.taskName == null) {
            throw new Exception("you must execute name before execute start function ");
        }
        return this.configurableComponent.queryConfigurableByType(StreamTask.TYPE);
    }

    @Override public void start() throws Exception {
        startSql(false);
    }

    @Override void stop() throws Exception {
        if (isStart.compareAndSet(true, false)) {
            this.streamsTask.destroy();
        }
    }

    public void asyncStart() {
        startSql(true);
    }

    protected void startSql(boolean isAsync) {
        if (isStart.compareAndSet(false, true)) {
            this.streamsTask.start();
            if (!isAsync) {
                while (true) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

}
