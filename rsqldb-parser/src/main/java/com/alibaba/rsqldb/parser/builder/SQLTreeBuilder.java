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
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.sql.parser.ddl.SqlNodeInfo;
import org.apache.flink.sql.parser.util.SqlContextUtils;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.common.metadata.MetaDataField;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.common.topology.model.PipelineSourceJoiner;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.configurable.ConfigurableComponent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SQLTreeBuilder {

    private static final Log LOG = LogFactory.getLog(SQLTreeBuilder.class);

    protected String namespace;
    protected String pipelineName;//pipeline Name
    protected String sql;//带解析的sql
    protected boolean shareSource = true;//是否共享数据源，如果共享，则根据channel表名在sourceutil中找对应的数据源，如果没有，把channel name当数据源
    protected Map<String, String> sourceNames;//数据源表名和数据源名字的映射关系，在多表join的场景应用
    protected String sourceName;//如果是单表，可以直接指定
    protected String sourcePipelineName;//数据源的pipeline name
    protected ConfigurableComponent configurableComponent;

    protected List<PipelineBuilder> pipelineBuilders = new ArrayList<>();//pipeline builder

    public SQLTreeBuilder(String namespace, String pipelineName, String sql, boolean shareSource, String sourceName, Map<String, String> sourceNames) {
        this.namespace = namespace;
        this.pipelineName = pipelineName;
        this.sql = sql.replace("\\\\", "\\");
        this.shareSource = shareSource;
        this.sourceNames = sourceNames;
        this.sourceName = sourceName;
        this.configurableComponent = ComponentCreator.getComponent(namespace, ConfigurableComponent.class);
    }

    public SQLTreeBuilder(String namespace, String pipelineName, String sql) {
        this.namespace = namespace;
        this.pipelineName = pipelineName;
        this.sql = sql.replace("\\\\", "\\");
        this.shareSource = false;
        this.configurableComponent = ComponentCreator.getComponent(namespace, ConfigurableComponent.class);
    }

    public SQLTreeBuilder(String namespace, String pipelineName, String sql, String sourcePipelineName) {
        this.namespace = namespace;
        this.pipelineName = pipelineName;
        this.sql = sql.replace("\\\\", "\\");
        this.sourcePipelineName = sourcePipelineName;
        this.configurableComponent = ComponentCreator.getComponent(namespace, ConfigurableComponent.class);
    }

    public SQLTreeBuilder(String namespace, String pipelineName, String sql, ConfigurableComponent configurableComponent) {
        this.namespace = namespace;
        this.pipelineName = pipelineName;
        this.sql = sql.replace("\\\\", "\\");
        this.shareSource = false;
        this.configurableComponent = configurableComponent;
    }

    /**
     * 把整个sql解析成多棵树，后续会基于树做规则生成
     *
     * @return
     */
    public List<ChainPipeline> build() {
        //把sql编译成sql builder
        List<ISQLBuilder> sqlBuilders = parseSQL();
        //把sql builder 编译成sql tree
        List<SQLTree> trees = buildSQLBuilder(sqlBuilders);
        //把sql tree 编译成chain pipeline（configurable service基于配置文件生成）
        return buildSQLTree(trees);
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
                AbstractSQLBuilder builder = null;
                ISqlParser sqlParser = SQLNodeParserFactory.getParse(sqlNode);
                if (sqlParser != null) {
                    IBuilderCreator creator = (IBuilderCreator)sqlParser;
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
                    CreateSQLBuilder createSQLBuilder = (CreateSQLBuilder)builder;
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
            CreateSQLBuilder createSQLBuilder = (CreateSQLBuilder)builder;
            List<MetaDataField> fields = createSQLBuilder.getMetaData().getMetaDataFields();
            for (MetaDataField field : fields) {
                fieldNames.add(field.getFieldName());
            }

        }
        if (builder instanceof ViewSQLBuilder) {
            ViewSQLBuilder viewSQLBuilder = (ViewSQLBuilder)builder;
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
            AbstractSQLBuilder sqlBuilder = (AbstractSQLBuilder)builder;
            // setTreeSQLBulider(sqlBuilder);
            sqlBuilder.setNamespace(namespace);
            String createTable = builder.getCreateTable();
            if (StringUtil.isNotEmpty(createTable)) {
                createTables.put(createTable, sqlBuilder);
            }
            if (builder instanceof FunctionSQLBuilder) {
                functionBuilders.add((FunctionSQLBuilder)builder);
                continue;
            }
            Set<String> dependentTables = builder.parseDependentTables();
            if (dependentTables.size() == 0) {
                ((AbstractSQLBuilder)builder).addRootTableName(((AbstractSQLBuilder<?>)builder).getTableName());
                continue;
            }
            /**
             * 如果是insert 节点，把对应的输出源节点设置进去
             */
            if (builder instanceof InsertSQLBuilder) {
                AbstractSQLBuilder createSQLBuilder = createTables.get(((InsertSQLBuilder)builder).getTableName());
                InsertSQLBuilder insertSQLBuilder = (InsertSQLBuilder)builder;
                insertSQLBuilder.setCreateBuilder((CreateSQLBuilder)createSQLBuilder);
            }
            for (String dependentName : dependentTables) {
                AbstractSQLBuilder parent = createTables.get(dependentName);
                //如果无父节点，把parent节点设置为跟节点
                if (parent == null) {
                    roots.put(sqlBuilder.getTableName(), sqlBuilder);

                    continue;
                }
                parent.addChild(sqlBuilder);
                ((AbstractSQLBuilder)builder).addRootTableName(parent.getRootTableNames());
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

    /**
     * 把多棵树解析成多个pipline，生成元数据和sql在成员变量中 shareSource 是否共享数据源，如果共享，则根据channel的表名，来寻找共同的source，如果找到，则创建数据源joiner对象，否则是独立的pipline。 *        需要保障数据源对象的sql会提前创建
     *
     * @return pipline list
     */
    public List<ChainPipeline> buildSQLTree(List<SQLTree> sqlTrees) {
        if (sqlTrees == null || sqlTrees.size() == 0) {
            return null;
        }
        List<ChainPipeline> piplines = new ArrayList<>();
        for (SQLTree sqlTree : sqlTrees) {
            PipelineBuilder builder = sqlTree.getRootCreator();
            if (shareSource) {
                String tableName = sqlTree.getRootTableName();
                String sourceName = getSourceName(tableName);
                if (StringUtil.isEmpty(sourceName)) {
                    sourceName = tableName;
                } else {
                    //如果有内置的数据源，创建连接对象，用来数据源动态加载这个pipeline
                    PipelineSourceJoiner pipelineSourceJoiner = new PipelineSourceJoiner();
                    pipelineSourceJoiner.setNameSpace(namespace);
                    pipelineSourceJoiner.setConfigureName(builder.getPipelineName());
                    pipelineSourceJoiner.setSourcePipelineName(sourceName);
                    pipelineSourceJoiner.setPipelineName(builder.getPipelineName());
                    sqlTree.getRootCreator().addConfigurables(pipelineSourceJoiner);
                }
                builder.getPipeline().setSourceIdentification(sourceName);
            }
            this.pipelineBuilders.add(builder);
            ChainPipeline pipline = builder.build(configurableComponent.getService());
            piplines.add(pipline);
        }
        return piplines;
    }

    /**
     * 获取source name。如果是多表join，需要把每个表名和sourcename的映射关系映射进来。 如果是单表，则可以直接返回
     *
     * @param tableName
     * @return
     */
    protected String getSourceName(String tableName) {
        String name = null;
        if (sourceNames != null) {
            name = this.sourceNames.get(tableName);
        }
        if (StringUtil.isEmpty(name)) {
            return this.sourceName;
        }
        return name;
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

    public boolean isShareSource() {
        return shareSource;
    }

    public void setShareSource(boolean shareSource) {
        this.shareSource = shareSource;
    }

    public Map<String, String> getSourceNames() {
        return sourceNames;
    }

    public void setSourceNames(Map<String, String> sourceNames) {
        this.sourceNames = sourceNames;
    }

    public String getSourceName() {
        return sourceName;
    }

    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    public String getSourcePipelineName() {
        return sourcePipelineName;
    }

    public void setSourcePipelineName(String sourcePipelineName) {
        this.sourcePipelineName = sourcePipelineName;
    }

    public ConfigurableComponent getConfigurableComponent() {
        return configurableComponent;
    }

    public void setConfigurableComponent(ConfigurableComponent configurableComponent) {
        this.configurableComponent = configurableComponent;
    }
}
