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
package com.alibaba.rsqldb.parser.sql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.alibaba.rsqldb.parser.ISqlNodeParser;
import com.alibaba.rsqldb.parser.SqlNodeParserFactory;
import com.alibaba.rsqldb.parser.builder.AbstractSqlBuilder;
import com.alibaba.rsqldb.parser.builder.CreateSqlBuilder;
import com.alibaba.rsqldb.parser.builder.FunctionSqlBuilder;
import com.alibaba.rsqldb.parser.builder.ISqlNodeBuilder;
import com.alibaba.rsqldb.parser.builder.InsertSqlBuilder;
import com.alibaba.rsqldb.parser.builder.NotSupportSqlBuilder;
import com.alibaba.rsqldb.parser.builder.ViewSqlBuilder;
import com.alibaba.rsqldb.parser.sql.context.CreateSqlBuildersForSqlTreeContext;
import com.alibaba.rsqldb.parser.sql.context.FieldsOfTableForSqlTreeContext;
import com.alibaba.rsqldb.parser.sqlnode.IBuilderCreator;

import org.apache.calcite.sql.SqlNode;
import org.apache.rocketmq.streams.common.metadata.MetaDataField;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.common.topology.graph.JobGraph;
import org.apache.rocketmq.streams.common.topology.model.ChainPipeline;
import org.apache.rocketmq.streams.common.utils.StringUtil;

;

public abstract class AbstractSqlParser implements ISqlParser {

    protected Properties jobConfiguration;

    @Override
    public JobGraph parse(String namespace, String jobName, String sql, Properties environmentConfiguration) {
        this.jobConfiguration = environmentConfiguration;
        sql = sql.replace("\\\\", "\\");
        //SQLParseContext.setNameCreator(new NameCreator());
        //SQLParseContext.setProperties(environmentConfiguration);
        //部分采集任务重没有createFunction, 而直接使用了data_time_format函数,为了兼容线上任务， 这里默认将udf申明加了进来
        sql = "CREATE FUNCTION eval AS 'org.apache.rocketmq.streams.script.function.impl.eval.EvalFunction';\n"
            + "CREATE FUNCTION date_time_format as 'com.aliyun.security.cloud.compute.binkudf.DateTimeZoneFormat';\n" + sql;
        //用于组建自动创建名字
        //把Sql编译成Sql builder
        List<ISqlNodeBuilder> sqlBuilders = parseSql2SqlNodeBuilder(sql, environmentConfiguration);
        //把Sql builder 编译成Sql tree
        List<SqlTree> trees = parserSqlTree(namespace, jobName, sqlBuilders);
        //把Sql tree 编译成chain pipeline（configurable service基于配置文件生成）
        List<ChainPipeline<?>> chainPipelines = buildSqlTree(trees);
        //SQLParseContext.removeNameCreator();
        //SQLParseContext.removeProperties();
        return new JobGraph(namespace, jobName, chainPipelines);
    }

    /**
     * 解析Sql，每一段Sql解析成一个Sql builder
     */
    protected List<ISqlNodeBuilder> parseSql2SqlNodeBuilder(String sql, Properties configuration) {
        SqlNode sqlNode = null;
        try {

            //按Sql声明顺序保存 Sql builder
            List<ISqlNodeBuilder> builders = new ArrayList<>();
            List<SqlNode> sqlNodeInfoList = parseSql(sql);
            CreateSqlBuildersForSqlTreeContext.setParserCreateSqlBuilder(builders);
            //在某些地方会用到上游节点提供的字段，放到localthread，便于获取和查看
            Map<String, Set<String>> tableName2Fields = new HashMap<>(8);
            FieldsOfTableForSqlTreeContext.getInstance().set(tableName2Fields);
            //在insert场景，需要获取insert对应表的声明信息，可以在insert builder通过local thread根据表名获取createSqlbuilder 对象
            Map<String, CreateSqlBuilder> createBuilders = new HashMap<>(8);
            CreateSqlBuildersForSqlTreeContext.getInstance().set(createBuilders);

            for (SqlNode node : sqlNodeInfoList) {
                sqlNode = node;
                AbstractSqlBuilder builder = null;
                ISqlNodeParser<SqlNode, AbstractSqlBuilder> sqlParser = SqlNodeParserFactory.getParse(node);
                if (sqlParser != null) {
                    IBuilderCreator<?> creator = (IBuilderCreator<?>)sqlParser;
                    builder = creator.create(configuration);
                    builder.setSqlNode(node);
                    sqlParser.parse(builder, node);
                } else {
                    builder = new NotSupportSqlBuilder();
                    builder.setSqlNode(node);
                }

                //获取输出的字段列表
                Set<String> fieldNames = getOutputFieldNames(builder);
                if (fieldNames.size() > 0) {
                    tableName2Fields.put(builder.getTableName(), fieldNames);
                }
                //设置所有的create builder
                if (builder instanceof CreateSqlBuilder) {
                    CreateSqlBuilder createSqlBuilder = (CreateSqlBuilder)builder;
                    createBuilders.put(builder.getTableName(), createSqlBuilder);
                }

                //builder中添加configuration，用于替换变量
                builder.setConfiguration(configuration);

                builders.add(builder);
            }
            return builders;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("SqlNode parser error " + sqlNode, e);
        }
    }

    /**
     * parse the sql to sql node list
     *
     * @param sql sql
     * @return sql node list
     */
    protected abstract List<SqlNode> parseSql(String sql);

    /**
     * 一段Sql最终输出的字段有哪些，具备输出字段能力的有create table和create view 两种Sql
     */
    protected Set<String> getOutputFieldNames(AbstractSqlBuilder builder) {
        Set<String> fieldNames = new HashSet<>();
        if (builder instanceof CreateSqlBuilder) {
            CreateSqlBuilder createSqlBuilder = (CreateSqlBuilder)builder;
            List<MetaDataField<?>> fields = createSqlBuilder.getMetaData().getMetaDataFields();
            for (MetaDataField<?> field : fields) {
                fieldNames.add(field.getFieldName());
            }
        }
        if (builder instanceof ViewSqlBuilder) {
            ViewSqlBuilder viewSqlBuilder = (ViewSqlBuilder)builder;
            for (String filedName : viewSqlBuilder.getFieldNames()) {
                String name = filedName;
                if (name.contains(".")) {
                    int startIndex = filedName.indexOf(".");
                    name = filedName.substring(startIndex + 1);
                }
                fieldNames.add(name);
            }
        }
        return fieldNames;
    }

    /**
     * 把 builder按表的依赖关系解析成多棵树，每棵树用SqlTree表达 普通Sql只有一个tree，双流join场景会有多个tree
     */
    protected List<SqlTree> parserSqlTree(String namespace, String jobName, List<ISqlNodeBuilder> sqlBuilders) {
        //函数builder
        List<FunctionSqlBuilder> functionBuilders = new ArrayList<>();
        //创建的表
        Map<String, AbstractSqlBuilder> createTables = new HashMap<>(8);
        //树的跟节点，多棵树有多个跟节点
        Map<String, AbstractSqlBuilder> roots = new HashMap<>(8);
        //
        for (ISqlNodeBuilder builder : sqlBuilders) {
            AbstractSqlBuilder sqlBuilder = (AbstractSqlBuilder)builder;
            sqlBuilder.setNamespace(namespace);
            String createTable = builder.getCreateTable();
            if (StringUtil.isNotEmpty(createTable)) {
                createTables.put(createTable, sqlBuilder);
            }
            if (builder instanceof FunctionSqlBuilder) {
                FunctionSqlBuilder functionSqlBuilder = (FunctionSqlBuilder)builder;
                functionBuilders.add(functionSqlBuilder);
                continue;
            }
            Set<String> dependentTables = builder.parseDependentTables();
            if (dependentTables.size() == 0) {
                ((AbstractSqlBuilder)builder).addRootTableName(((AbstractSqlBuilder)builder).getTableName());
                continue;
            }
            //如果是insert 节点，把对应的输出源节点设置进去
            if (builder instanceof InsertSqlBuilder) {
                AbstractSqlBuilder createSqlBuilder = createTables.get(((InsertSqlBuilder)builder).getTableName());
                InsertSqlBuilder insertSqlBuilder = (InsertSqlBuilder)builder;
                insertSqlBuilder.setCreateBuilder((CreateSqlBuilder)createSqlBuilder);
            }
            for (String dependentName : dependentTables) {
                AbstractSqlBuilder parent = createTables.get(dependentName);
                //如果无父节点，把parent节点设置为跟节点
                if (parent == null) {
                    roots.put(sqlBuilder.getTableName(), sqlBuilder);
                    continue;
                }
                parent.addChild(sqlBuilder);
                ((AbstractSqlBuilder)builder).addRootTableName(parent.getRootTableNames());
                boolean isRoot = isRoot(parent);
                if (isRoot) {
                    parent.setNamespace(namespace);
                    roots.put(parent.getTableName(), parent);
                }
            }
        }

        //每个跟节点对应一颗树
        List<SqlTree> sqlTrees = new ArrayList<>();
        int i = 0;
        for (AbstractSqlBuilder sqlBuilder : roots.values()) {
            String name = jobName;
            if (i > 0) {
                name = name + "_" + i;
            }
            SqlTree sqlTree = new SqlTree(name, sqlBuilder, functionBuilders, jobConfiguration);
            sqlTree.build();
            sqlTrees.add(sqlTree);
            i++;
        }

        return sqlTrees;
    }

    /**
     * 把多棵树解析成多个pipeline，生成元数据和Sql在成员变量中 shareSource 是否共享数据源，如果共享，则根据channel的表名，来寻找共同的source，如果找到，则创建数据源joiner对象，否则是独立的pipeline。 *        需要保障数据源对象的Sql会提前创建
     */
    public List<ChainPipeline<?>> buildSqlTree(List<SqlTree> sqlTrees) {
        if (sqlTrees == null || sqlTrees.size() == 0) {
            return null;
        }
        List<ChainPipeline<?>> chainPipelines = new ArrayList<>();
        for (SqlTree sqlTree : sqlTrees) {
            PipelineBuilder builder = sqlTree.getRootPipelineBuilder();

            ChainPipeline<?> pipeline = builder.build();
            chainPipelines.add(pipeline);
            pipeline.setChannelMetaData(builder.getChannelMetaData());
        }
        return chainPipelines;
    }

    /**
     * 判断一个节点是否是root节点
     *
     * @param builder parent
     * @return boolean
     */
    private boolean isRoot(AbstractSqlBuilder builder) {
        if (builder.getParents() != null && builder.getParents().size() > 0) {
            return false;
        }
        return builder instanceof CreateSqlBuilder;
    }

}
