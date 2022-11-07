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

import com.alibaba.rsqldb.parser.parser.ISqlNodeParser;
import com.alibaba.rsqldb.parser.parser.SqlNodeParserFactory;
import com.alibaba.rsqldb.parser.parser.builder.AbstractSqlBuilder;
import com.alibaba.rsqldb.parser.parser.builder.CreateSqlBuilder;
import com.alibaba.rsqldb.parser.parser.builder.FunctionSqlBuilder;
import com.alibaba.rsqldb.parser.parser.builder.ISqlNodeBuilder;
import com.alibaba.rsqldb.parser.parser.builder.InsertSqlBuilder;
import com.alibaba.rsqldb.parser.parser.builder.NotSupportSqlBuilder;
import com.alibaba.rsqldb.parser.parser.builder.ViewSqlBuilder;
import com.alibaba.rsqldb.parser.parser.sqlnode.IBuilderCreator;
import com.alibaba.rsqldb.parser.sql.context.CreateSqlBuildersForSqlTreeContext;
import com.alibaba.rsqldb.parser.sql.context.FieldsOfTableForSqlTreeContext;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.sql.SqlNode;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.common.metadata.MetaDataField;
import org.apache.rocketmq.streams.common.model.NameCreator;
import org.apache.rocketmq.streams.common.model.NameCreatorContext;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.configurable.ConfigurableComponent;
import org.apache.rocketmq.streams.tasks.StreamTask;

public abstract class AbstractSqlParser implements ISqlParser {

    protected String namespace;
    protected String pipelineName;
    protected String sql;

    protected ConfigurableComponent configurableComponent;

    public AbstractSqlParser(String namespace, String pipelineName, String sql) {
        this(namespace, pipelineName, sql, null);
    }

    public AbstractSqlParser(String namespace, String pipelineName, String sql, ConfigurableComponent component) {
        this.namespace = namespace;
        this.pipelineName = pipelineName;
        this.sql = sql.replace("\\\\", "\\");
        if (component != null) {
            this.configurableComponent = component;
        } else {
            this.configurableComponent = ComponentCreator.getComponent(namespace, ConfigurableComponent.class);
        }
    }

    private List<IConfigurable> configurables;
    private List<ChainPipeline<?>> chainPipelines;

    @Override public ISqlParser parse(boolean isBuild) {
        //用于组建自动创建名字
        NameCreatorContext.init(new NameCreator());
        //把Sql编译成Sql builder
        List<ISqlNodeBuilder> sqlBuilders = parseSql2SqlNodeBuilder();
        //把Sql builder 编译成Sql tree
        List<SqlTree> trees = parserSqlTree(sqlBuilders);
        //把Sql tree 编译成chain pipeline（configurable service基于配置文件生成）
        buildSqlTree(trees, isBuild);
        NameCreatorContext.remove();
        return this;
    }

    @Override public List<ChainPipeline<?>> pipelines() {
        return this.chainPipelines;
    }

    @Override public List<IConfigurable> configurables() {
        return this.configurables;
    }

    /**
     * 解析Sql，每一段Sql解析成一个Sql builder
     */
    protected List<ISqlNodeBuilder> parseSql2SqlNodeBuilder() {
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
                AbstractSqlBuilder<?> builder = null;
                ISqlNodeParser sqlParser = SqlNodeParserFactory.getParse(sqlNode);
                if (sqlParser != null) {
                    IBuilderCreator<?> creator = (IBuilderCreator<?>) sqlParser;
                    builder = creator.create();
                    builder.setSqlNode(sqlNode);
                    sqlParser.parse(builder, sqlNode);
                } else {
                    builder = new NotSupportSqlBuilder();
                    builder.setSqlNode(sqlNode);
                }

                //获取输出的字段列表
                Set<String> fieldNames = getOutputFieldNames(builder);
                if (fieldNames.size() > 0) {
                    tableName2Fields.put(builder.getTableName(), fieldNames);
                }
                //设置所有的create builder
                if (builder instanceof CreateSqlBuilder) {
                    CreateSqlBuilder createSqlBuilder = (CreateSqlBuilder) builder;
                    createBuilders.put(builder.getTableName(), createSqlBuilder);
                }
                builders.add(builder);
            }
            return builders;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Sqlnode parser error " + sqlNode, e);
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
    protected Set<String> getOutputFieldNames(AbstractSqlBuilder<?> builder) {
        Set<String> fieldNames = new HashSet<>();
        if (builder instanceof CreateSqlBuilder) {
            CreateSqlBuilder createSqlBuilder = (CreateSqlBuilder) builder;
            List<MetaDataField<?>> fields = createSqlBuilder.getMetaData().getMetaDataFields();
            for (MetaDataField<?> field : fields) {
                fieldNames.add(field.getFieldName());
            }
        }
        if (builder instanceof ViewSqlBuilder) {
            ViewSqlBuilder viewSqlBuilder = (ViewSqlBuilder) builder;
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
    protected List<SqlTree> parserSqlTree(List<ISqlNodeBuilder> sqlBuilders) {
        //函数builder
        List<FunctionSqlBuilder> functionBuilders = new ArrayList<>();
        //创建的表
        Map<String, AbstractSqlBuilder<?>> createTables = new HashMap<>(8);
        //树的跟节点，多棵树有多个跟节点
        Map<String, AbstractSqlBuilder<?>> roots = new HashMap<>(8);
        //
        for (ISqlNodeBuilder builder : sqlBuilders) {
            AbstractSqlBuilder<?> sqlBuilder = (AbstractSqlBuilder<?>) builder;
            sqlBuilder.setNamespace(namespace);
            String createTable = builder.getCreateTable();
            if (StringUtil.isNotEmpty(createTable)) {
                createTables.put(createTable, sqlBuilder);
            }
            if (builder instanceof FunctionSqlBuilder) {
                functionBuilders.add((FunctionSqlBuilder) builder);
                continue;
            }
            Set<String> dependentTables = builder.parseDependentTables();
            if (dependentTables.size() == 0) {
                ((AbstractSqlBuilder<?>) builder).addRootTableName(((AbstractSqlBuilder<?>) builder).getTableName());
                continue;
            }
            //如果是insert 节点，把对应的输出源节点设置进去
            if (builder instanceof InsertSqlBuilder) {
                AbstractSqlBuilder<?> createSqlBuilder = createTables.get(((InsertSqlBuilder) builder).getTableName());
                InsertSqlBuilder insertSqlBuilder = (InsertSqlBuilder) builder;
                insertSqlBuilder.setCreateBuilder((CreateSqlBuilder) createSqlBuilder);
            }
            for (String dependentName : dependentTables) {
                AbstractSqlBuilder<?> parent = createTables.get(dependentName);
                //如果无父节点，把parent节点设置为跟节点
                if (parent == null) {
                    roots.put(sqlBuilder.getTableName(), sqlBuilder);
                    continue;
                }
                parent.addChild(sqlBuilder);
                ((AbstractSqlBuilder<?>) builder).addRootTableName(parent.getRootTableNames());
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
        for (AbstractSqlBuilder<?> sqlBuilder : roots.values()) {
            String name = pipelineName;
            if (i > 0) {
                name = name + "_" + i;
            }
            SqlTree sqlTree = new SqlTree(name, sqlBuilder, functionBuilders);
            sqlTree.build();
            sqlTrees.add(sqlTree);
            i++;
        }

        return sqlTrees;
    }

    /**
     * 把多棵树解析成多个pipeline，生成元数据和Sql在成员变量中 shareSource 是否共享数据源，如果共享，则根据channel的表名，来寻找共同的source，如果找到，则创建数据源joiner对象，否则是独立的pipeline。 *        需要保障数据源对象的Sql会提前创建
     */
    public void buildSqlTree(List<SqlTree> sqlTrees, boolean isNeedBuild) {
        if (sqlTrees == null || sqlTrees.size() == 0) {
            return;
        }
        this.configurables = Lists.newArrayList();
        this.chainPipelines = Lists.newArrayList();

        IConfigurableService configurableService = configurableComponent.getService();
        for (SqlTree sqlTree : sqlTrees) {
            PipelineBuilder builder = sqlTree.getRootPipelineBuilder();
            ChainPipeline<?> chainPipeline = null;
            if (isNeedBuild) {
                chainPipeline = builder.build(configurableService);
            } else {
                chainPipeline = builder.getPipeline();
            }
            this.chainPipelines.add(chainPipeline);
            this.configurables.addAll(builder.getAllConfigurables());
        }
    }

    @Override public StreamTask streamTask() {
        StreamTask streamTask = new StreamTask();
        streamTask.setNameSpace(this.namespace);
        streamTask.setConfigureName(this.pipelineName);
        streamTask.setUpdateFlag(0);
        streamTask.setPipelines(pipelines());
        return streamTask;
    }

    /**
     * 判断一个节点是否是root节点
     *
     * @param builder parent
     * @return boolean
     */
    private boolean isRoot(AbstractSqlBuilder<?> builder) {
        if (builder.getParents() != null && builder.getParents().size() > 0) {
            return false;
        }
        return builder instanceof CreateSqlBuilder;
    }

}
