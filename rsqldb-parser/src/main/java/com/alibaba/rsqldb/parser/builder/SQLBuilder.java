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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configurable.AbstractConfigurable;
import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.configurable.ConfigurableComponent;
import com.alibaba.rsqldb.parser.parser.builder.BlinkUDFScan;

public class SQLBuilder {
    protected String namespace;
    protected String pipelineName;
    protected String sql;
    protected volatile boolean isStop = false;//设置这个值，停止主线程运行
    protected AtomicBoolean hasBuilder = new AtomicBoolean(false);//可重入保障
    protected List<PipelineBuilder> pipelineBuilders;//
    protected List<ChainPipeline> chainPipelines;//编译后的拓扑结构，可以直接运行。一个sql一个，如果有join场景，会有多个

    public SQLBuilder(String namespace, String pipelineName, String sql) {
        this.namespace = namespace;
        this.pipelineName = pipelineName;
        this.sql = sql;
    }

    /**
     * 扫描某个目录下jar包的包名
     *
     * @param jarDir       如果为null，在类路径扫描
     * @param packageNames
     */
    public void registerUDFPackage(String jarDir, String... packageNames) {
        if (packageNames != null) {
            for (String packageName : packageNames) {
                BlinkUDFScan.getInstance().scan(jarDir, packageName);
            }
        }
    }

    /**
     * 编译成pipeline
     *
     * @return
     */
    public void build(ConfigurableComponent component) {
        if (hasBuilder.compareAndSet(false, true)) {
            SQLTreeBuilder sqlTreeBuilder = createSQLTreeBuilder(component);
            List<ChainPipeline> chainPipelines = sqlTreeBuilder.build();
            pipelineBuilders = sqlTreeBuilder.getPipelineBuilders();
            this.chainPipelines = chainPipelines;

        }

    }
    /**
     * 编译成pipeline
     *
     * @return
     */
    public void build() {
       build(null);

    }


    protected SQLTreeBuilder createSQLTreeBuilder(
        ConfigurableComponent component) {
        SQLTreeBuilder sqlTreeBuilder= new SQLTreeBuilder(namespace, pipelineName, sql);
        if(component!=null){
            sqlTreeBuilder.setConfigurableComponent(component);
        }
        return sqlTreeBuilder;
    }

    /**
     * 把这个sql生成的所有元数据保存在configurables，并把元数据生成sqlList，保存在sql中
     */
    public List<String> getInsertSql() {
        build();
        List<IConfigurable> allConfigurables = new ArrayList<>();
        for (PipelineBuilder pipelineBuilder : pipelineBuilders) {
            allConfigurables.addAll(pipelineBuilder.getAllConfigurables());
        }

        List<String> sqlList = new ArrayList<>();
        String tableName = ComponentCreator.getProperties().getProperty("dipper.rds.table.name");
        for (IConfigurable configurable : allConfigurables) {
            AbstractConfigurable abstractConfigurable = (AbstractConfigurable)configurable;
            String sql = null;
            if (StringUtil.isNotEmpty(tableName)) {
                sql = AbstractConfigurable.createSQL(abstractConfigurable, tableName);
            } else {
                sql = abstractConfigurable.createSQL();
            }
            sqlList.add(sql);
        }
        return sqlList;
    }

    /**
     * 开始跑任务
     */
    public void startSQL() {
        build();
        for (ChainPipeline pipeline : chainPipelines) {
            pipeline.startChannel();
        }
    }

    public List<PipelineBuilder> getPipelineBuilders() {
        build();
        return pipelineBuilders;
    }

    public List<ChainPipeline> getChainPipelines() {
        build();
        return chainPipelines;
    }
}
