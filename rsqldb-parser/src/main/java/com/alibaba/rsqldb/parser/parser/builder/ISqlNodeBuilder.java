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
package com.alibaba.rsqldb.parser.parser.builder;

import com.alibaba.rsqldb.parser.parser.SqlBuilderResult;
import java.util.Set;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;

/**
 * 把一条独立sql对应的描述信息保存下来，并能够builder成dipper的pipeline的节点
 */
public interface ISqlNodeBuilder {

    /**
     * 解析的SQL是select，create view，create
     *
     * @return
     */
    String getSQLType();

    /**
     * 创建优化后的sql
     *
     * @return
     */
    String createSql();

    /**
     * 把sql编译成pipline中的组件
     */
    SqlBuilderResult buildSql();

    /**
     * 是否支持优化，如果不支持优化，则直接返回原来的sql
     *
     * @return
     */
    boolean supportOptimization();

    /**
     * 解析出所有依赖表，比如select中用到的表，就是依赖
     *
     * @return
     */
    Set<String> parseDependentTables();

    //    Set<String> parseDependentFields();//解析出所有依赖的字段，只考虑where条件，比如where用到了某个字段，则被视为依赖

    /**
     * 获取产生的新表表名，主要用在create table，create view中
     *
     * @return
     */
    String getCreateTable();

    void setPipelineBuilder(PipelineBuilder pipelineBuilder);

    //给一系列字段，判断当前字段是否是本节点产出的字段，如果是把字段完成脚本解析后返回
    //    List<String> paserField2Script(Set<String> scriptDependentFields,Map<String,List<AbstractSQLBuilder>>
    //    field2Descriptors);

}
