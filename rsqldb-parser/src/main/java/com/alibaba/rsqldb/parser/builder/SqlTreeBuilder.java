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

import com.alibaba.rsqldb.parser.parser.ISqlParser;
import com.alibaba.rsqldb.parser.parser.SQLNodeParserFactory;
import com.alibaba.rsqldb.parser.parser.SQLParserContext;
import com.alibaba.rsqldb.parser.parser.SQLTree;
import com.alibaba.rsqldb.parser.parser.builder.AbstractSQLBuilder;
import com.alibaba.rsqldb.parser.parser.builder.CreateSQLBuilder;
import com.alibaba.rsqldb.parser.parser.builder.FunctionSQLBuilder;
import com.alibaba.rsqldb.parser.parser.builder.ISQLBuilder;
import com.alibaba.rsqldb.parser.parser.builder.InsertSQLBuilder;
import com.alibaba.rsqldb.parser.parser.builder.NotSupportSQLBuilder;
import com.alibaba.rsqldb.parser.parser.builder.SQLCreateTables;
import com.alibaba.rsqldb.parser.parser.builder.ViewSQLBuilder;
import com.alibaba.rsqldb.parser.parser.sqlnode.IBuilderCreator;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.sql.parser.ddl.SqlNodeInfo;
import org.apache.flink.sql.parser.util.SqlContextUtils;
import org.apache.rocketmq.streams.common.channel.impl.view.ViewSource;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.common.metadata.MetaDataField;
import org.apache.rocketmq.streams.common.model.NameCreator;
import org.apache.rocketmq.streams.common.model.NameCreatorContext;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.common.topology.task.StreamsTask;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.configurable.ConfigurableComponent;

public class SqlTreeBuilder {

    private static final Log LOG = LogFactory.getLog(SqlTreeBuilder.class);

    protected String namespace;
    protected String pipelineName;
    protected String sql;

    protected ConfigurableComponent configurableComponent;
    /**
     * pipeline builder
     */
    protected List<PipelineBuilder> pipelineBuilders = new ArrayList<>();

    /**
     * streamsTask实例
     */
    private StreamsTask streamsTask;

    @Deprecated protected boolean isCreateStreamTask = true;//默认不用修改，在专有云场景会用这个字段控制是否生成streamtask

    public SqlTreeBuilder(String namespace, String pipelineName, String sql) {
        this.namespace = namespace;
        this.pipelineName = pipelineName;
        this.sql = sql.replace("\\\\", "\\");
        this.configurableComponent = ComponentCreator.getComponent(namespace, ConfigurableComponent.class);
    }

    public SqlTreeBuilder(String namespace, String pipelineName, String sql, ConfigurableComponent configurableComponent) {
        this.namespace = namespace;
        this.pipelineName = pipelineName;
        this.sql = sql.replace("\\\\", "\\");
        this.configurableComponent = configurableComponent;
    }

    public List<ChainPipeline<?>> build() {
        List<ChainPipeline<?>> pipelines=new ArrayList<>();
        build(true,false,pipelines);
        return pipelines;
    }

    public List<IConfigurable> build(Boolean isBuild) {
        return build(isBuild,false,null);
    }

    public List<IConfigurable> build(Boolean isBuild, Boolean isStreamTaskStart,List<ChainPipeline<?>> pipelineList) {
        NameCreatorContext.init(new NameCreator());
        //把sql编译成sql builder
        List<ISQLBuilder> sqlBuilders = parseSQL();
        //把sql builder 编译成sql tree
        List<SQLTree> trees = buildSQLBuilder(sqlBuilders);
        //把sql tree 编译成chain pipeline（configurable service基于配置文件生成）
        List<IConfigurable> configurables = Lists.newArrayList();
        List<ChainPipeline<?>> pipelines=buildSqlTree(trees, isBuild, isStreamTaskStart, configurables);
        if(pipelineList!=null){
            pipelineList.addAll(pipelines);
        }
        NameCreatorContext.remove();
        return configurables;
    }

    /**
     * 解析sql，每一段sql解析成一个sql builder
     */
    protected List<ISQLBuilder> parseSQL() {
        try {
            //按sql声明顺序保存 sql builder
            List<ISQLBuilder> builders = new ArrayList<>();
            List<SqlNodeInfo> sqlNodeInfoList = SqlContextUtils.parseContext(sql);

            //在某些地方会用到上游节点提供的字段，放到localthread，便于获取和查看
            Map<String, Set<String>> tableName2Fields = new HashMap<>(8);
            SQLParserContext.getInstance().set(tableName2Fields);
            //在insert场景，需要获取insert对应表的声明信息，可以在insert builder通过local thread根据表名获取createsqlbuilder 对象
            Map<String, CreateSQLBuilder> createBuilders = new HashMap<>(8);
            SQLCreateTables.getInstance().set(createBuilders);
            for (SqlNodeInfo sqlNodeInfo : sqlNodeInfoList) {
                SqlNode sqlNode = sqlNodeInfo.getSqlNode();
                AbstractSQLBuilder<?> builder = null;
                ISqlParser sqlParser = SQLNodeParserFactory.getParse(sqlNode);
                if (sqlParser != null) {
                    IBuilderCreator<?> creator = (IBuilderCreator<?>) sqlParser;
                    builder = creator.create();
                    builder.setSqlNode(sqlNode);
                    sqlParser.parse(builder, sqlNode);
                } else {
                    builder = new NotSupportSQLBuilder();
                    builder.setSqlNode(sqlNode);
                }

                //获取输出的字段列表
                Set<String> fieldNames = getOutputFieldNames(builder);
                if (fieldNames.size() > 0) {
                    tableName2Fields.put(builder.getTableName(), fieldNames);
                }
                //设置所有的create builder
                if (builder instanceof CreateSQLBuilder) {
                    CreateSQLBuilder createSQLBuilder = (CreateSQLBuilder) builder;
                    createBuilders.put(builder.getTableName(), createSQLBuilder);
                }
                builders.add(builder);
            }
            return builders;
        } catch (org.apache.flink.sql.parser.plan.SqlParseException e) {
            e.printStackTrace();
            throw new RuntimeException("sqlnode parser error " + sql, e);
        }
    }

    /**
     * 一段sql最终输出的字段有哪些，具备输出字段能力的有create table和create view 两种sql
     *
     * @param builder
     * @return
     */
    protected Set<String> getOutputFieldNames(AbstractSQLBuilder builder) {
        Set<String> fieldNames = new HashSet<>();
        if (builder instanceof CreateSQLBuilder) {
            CreateSQLBuilder createSQLBuilder = (CreateSQLBuilder) builder;
            List<MetaDataField> fields = createSQLBuilder.getMetaData().getMetaDataFields();
            for (MetaDataField field : fields) {
                fieldNames.add(field.getFieldName());
            }
        }
        if (builder instanceof ViewSQLBuilder) {
            ViewSQLBuilder viewSQLBuilder = (ViewSQLBuilder) builder;
            for (String filedName : viewSQLBuilder.getFieldNames()) {
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
     * 把 builder按表的依赖关系解析成多棵树，每棵树用SQLTree表达 普通sql只有一个tree，双流join场景会有多个tree
     *
     * @param sqlBuilders
     * @return
     */
    protected List<SQLTree> buildSQLBuilder(List<ISQLBuilder> sqlBuilders) {
        //函数builder
        List<FunctionSQLBuilder> functionBuilders = new ArrayList<>();
        //创建的表
        Map<String, AbstractSQLBuilder> createTables = new HashMap<>(8);
        //树的跟节点，多棵树有多个跟节点
        Map<String, AbstractSQLBuilder> roots = new HashMap<>(8);
        //
        for (ISQLBuilder builder : sqlBuilders) {
            AbstractSQLBuilder sqlBuilder = (AbstractSQLBuilder) builder;
            sqlBuilder.setNamespace(namespace);
            String createTable = builder.getCreateTable();
            if (StringUtil.isNotEmpty(createTable)) {
                createTables.put(createTable, sqlBuilder);
            }
            if (builder instanceof FunctionSQLBuilder) {
                functionBuilders.add((FunctionSQLBuilder) builder);
                continue;
            }
            Set<String> dependentTables = builder.parseDependentTables();
            if (dependentTables.size() == 0) {
                ((AbstractSQLBuilder) builder).addRootTableName(((AbstractSQLBuilder<?>) builder).getTableName());
                continue;
            }
            /**
             * 如果是insert 节点，把对应的输出源节点设置进去
             */
            if (builder instanceof InsertSQLBuilder) {
                AbstractSQLBuilder createSQLBuilder = createTables.get(((InsertSQLBuilder) builder).getTableName());
                InsertSQLBuilder insertSQLBuilder = (InsertSQLBuilder) builder;
                insertSQLBuilder.setCreateBuilder((CreateSQLBuilder) createSQLBuilder);
            }
            for (String dependentName : dependentTables) {
                AbstractSQLBuilder parent = createTables.get(dependentName);
                //如果无父节点，把parent节点设置为跟节点
                if (parent == null) {
                    roots.put(sqlBuilder.getTableName(), sqlBuilder);
                    continue;
                }
                parent.addChild(sqlBuilder);
                ((AbstractSQLBuilder) builder).addRootTableName(parent.getRootTableNames());
                boolean isRoot = isRoot(parent, sqlBuilder);
                if (isRoot) {
                    parent.setNamespace(namespace);
                    roots.put(parent.getTableName(), parent);
                }
            }
        }

        //每个跟节点对应一颗树
        List<SQLTree> sqlTrees = new ArrayList<>();
        int i = 0;
        for (AbstractSQLBuilder sqlBuilder : roots.values()) {
            String name = pipelineName;
            if (i > 0) {
                name = name + "_" + i;
            }
            SQLTree sqlTree = new SQLTree(name, sqlBuilder, functionBuilders);
            sqlTree.build();
            sqlTrees.add(sqlTree);
            i++;
        }

        return sqlTrees;
    }


    public List<ChainPipeline<?>> buildSqlTree(List<SQLTree> sqlTrees, boolean isStreamTaskStart) {
        return buildSqlTree(sqlTrees, true, isStreamTaskStart,null);
    }

    /**
     * 把多棵树解析成多个pipline，生成元数据和sql在成员变量中 shareSource 是否共享数据源，如果共享，则根据channel的表名，来寻找共同的source，如果找到，则创建数据源joiner对象，否则是独立的pipline。 *        需要保障数据源对象的sql会提前创建
     *
     * @return pipline list
     */
    public List<ChainPipeline<?>> buildSqlTree(List<SQLTree> sqlTrees, boolean isNeedBuild, boolean isStreamTaskStart, List<IConfigurable> configurables) {
        if (sqlTrees == null || sqlTrees.size() == 0) {
            return null;
        }
        IConfigurableService configurableService = configurableComponent.getService();
        List<ChainPipeline<?>> pipelines = new ArrayList<>();
        List<ChainPipeline<?>> pipelinesOfStreamTask = new ArrayList<>();
        for (SQLTree sqlTree : sqlTrees) {
            PipelineBuilder builder = sqlTree.getRootCreator();
            this.pipelineBuilders.add(builder);
            if (isNeedBuild) {
                ChainPipeline<?> chainPipeline = builder.build(configurableService);
                //If it is a view source, do not create a streamtask because it cannot be executed independently and must be merged into the sink
                if (chainPipeline.getSource() != null && !(chainPipeline.getSource() instanceof ViewSource)) {
                    pipelinesOfStreamTask.add(chainPipeline);
                }
                pipelines.add(chainPipeline);
            } else {
                //当不需要遍历时，直接获取当前的pipeline
                pipelines.add(builder.getPipeline());
                pipelinesOfStreamTask.add(builder.getPipeline());
            }
            if (configurables != null) {
                configurables.addAll(builder.getAllConfigurables());
            }
        }
        if (pipelinesOfStreamTask.size() == 0) {
            return pipelines;
        }
        if (!isCreateStreamTask) {
            return pipelines;
        }
        //获取streamsTask实例
        StreamsTask currentTask = null;
        List<StreamsTask> streamsTasks = configurableComponent.queryConfigurableByType(StreamsTask.TYPE);
        if (streamsTasks != null && !streamsTasks.isEmpty()) {
            streamsTasks = streamsTasks.stream().filter(task -> task.getConfigureName().equals(this.pipelineName)).collect(Collectors.toList());
        }
        if (streamsTasks != null && !streamsTasks.isEmpty()) {
            currentTask = streamsTasks.get(0);
        }
        StreamsTask copy = new StreamsTask();
        if (currentTask != null) {
            copy.toObject(currentTask.toJson());
            copy.setUpdateFlag(currentTask.getUpdateFlag() + 1);
        }
        if (isStreamTaskStart) {
            copy.setState(StreamsTask.STATE_STARTED);
        }
        copy.setPipelines(pipelinesOfStreamTask);
        copy.setConfigureName(this.pipelineName);
        copy.setNameSpace(this.namespace);
        configurableService.insert(copy);
        this.streamsTask = copy;
        if (configurables != null) {
            configurables.add(this.streamsTask);
        }

        return pipelines;
    }

    /**
     * 判断一个节点是否是root节点
     *
     * @param parent
     * @return
     */
    private boolean isRoot(AbstractSQLBuilder parent, AbstractSQLBuilder child) {
        if (parent.getParents() != null && parent.getParents().size() > 0) {
            return false;
        }
        return parent instanceof CreateSQLBuilder;
    }

    public List<PipelineBuilder> getPipelineBuilders() {
        return pipelineBuilders;
    }

    public List<IConfigurable> getConfigurables() {
        List<IConfigurable> allConfigurables = new ArrayList<>();
        for (PipelineBuilder pipelineBuilder : pipelineBuilders) {
            allConfigurables.addAll(pipelineBuilder.getAllConfigurables());
        }
        return allConfigurables;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getPipelineName() {
        return pipelineName;
    }

    public void setPipelineName(String pipelineName) {
        this.pipelineName = pipelineName;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public ConfigurableComponent getConfigurableComponent() {
        return configurableComponent;
    }

    public void setConfigurableComponent(ConfigurableComponent configurableComponent) {
        this.configurableComponent = configurableComponent;
    }

    public StreamsTask getStreamsTask() {
        return streamsTask;
    }

    public void setStreamsTask(StreamsTask streamsTask) {
        this.streamsTask = streamsTask;
    }

    public boolean isCreateStreamTask() {
        return isCreateStreamTask;
    }

    public void setCreateStreamTask(boolean createStreamTask) {
        isCreateStreamTask = createStreamTask;
    }
}
