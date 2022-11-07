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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.sql.SqlNode;
import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.common.topology.metric.StageGroup;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.common.utils.PrintUtil;
import org.apache.rocketmq.streams.common.utils.SQLFormatterUtil;

public abstract class AbstractSqlBuilder<T extends AbstractSqlBuilder> implements ISqlNodeBuilder {
    public static SQLFormatterUtil sqlFormatterUtil = new SQLFormatterUtil();
    /**
     * 解析节点对应的sqlnode
     */
    protected SqlNode sqlNode;
    /**
     * sql的类型，是查询还是create
     */
    protected String sqlType;
    protected List<AbstractSqlBuilder<?>> children = new ArrayList<>();
    protected List<AbstractSqlBuilder<?>> parents = new ArrayList<>();
    /**
     * 是否支持优化，可以在不支持解析的地方设置这个参数，builder会忽略对这个节点的优化
     */
    protected boolean supportOptimization = true;
    /**
     * 表名
     */
    protected String tableName;
    /**
     * 这个节点产生的新表
     */
    protected String createTable;
    /**
     * 命名空间
     */
    protected String namespace;
    protected PipelineBuilder pipelineBuilder;
    /**
     * 对于上层table的依赖
     */
    protected Set<String> dependentTables = new HashSet<>();
    /**
     * 表的别名，通过as修饰
     */
    protected String asName;
    /**
     * select，where部分有函数的，则把函数转换成脚本
     */
    protected List<String> scripts = new ArrayList<>();
    /**
     * 保存所有create对应的builder， 在insert或维表join时使用
     */
    protected Map<String, CreateSqlBuilder> tableName2Builders = new HashMap<>();
    protected HashSet<String> rootTableNames = new HashSet<>();
    protected String selectSql;
    protected String fromSql;
    protected String whereSql;
    protected String groupSql;
    protected String havingSql;
    protected String joinConditionSql;
    protected List<String> expressionFunctionSql = new ArrayList<>();

    @Override
    public String getSQLType() {
        return sqlType;
    }

    public void addCreatedTable(String tableName) {
        createTable = tableName;
    }

    @Override
    public boolean supportOptimization() {
        return supportOptimization;
    }

    public void addDependentTable(String tableName) {
        if (tableName != null) {
            dependentTables.add(tableName);
        }

    }

    public String createSQLFromParser() {
        StringBuilder sb = new StringBuilder();
        if (selectSql != null) {
            sb.append(selectSql).append(PrintUtil.LINE);
        }
        if (fromSql != null) {
            sb.append(fromSql).append(PrintUtil.LINE);
        }

        if (whereSql != null) {
            sb.append(whereSql).append(PrintUtil.LINE);
        }

        if (groupSql != null) {
            sb.append(groupSql).append(PrintUtil.LINE);
        }
        if (havingSql != null) {
            sb.append(havingSql).append(PrintUtil.LINE);
        }
        return sqlFormatterUtil.format(sb.toString());
    }

    @Override
    public String createSql() {
        return createSQLFromParser();
    }

    public SqlNode getSqlNode() {
        return sqlNode;
    }

    public void setSqlNode(SqlNode sqlNode) {
        this.sqlNode = sqlNode;
    }

    public String getSqlType() {
        return sqlType;
    }

    public void setSqlType(String sqlType) {
        this.sqlType = sqlType;
    }

    public List<AbstractSqlBuilder<?>> getChildren() {
        return children;
    }

    public void setChildren(List<AbstractSqlBuilder<?>> children) {
        this.children = children;
    }

    public void addChild(AbstractSqlBuilder<?> child) {
        this.children.add(child);
        child.getParents().add(this);
    }

    @Override
    public String getCreateTable() {
        return createTable;
    }

    @Override
    public SqlBuilderResult buildSql() {
        if (getPipelineBuilder() == null) {
            PipelineBuilder pipelineBuilder = findPipelineBuilderFromParent(this);
            setPipelineBuilder(pipelineBuilder);
        }
        if (getPipelineBuilder() == null) {
            pipelineBuilder = new PipelineBuilder(getNamespace(), getTableName());
        }

        build();
        return new SqlBuilderResult(pipelineBuilder, this);
    }

    protected PipelineBuilder findPipelineBuilderFromParent(AbstractSqlBuilder<?> sqlBuilder) {
        for (AbstractSqlBuilder<?> parent : sqlBuilder.getParents()) {
            if (parent.getPipelineBuilder() != null) {
                return parent.getPipelineBuilder();
            } else {
                PipelineBuilder pipelineBuilder = findPipelineBuilderFromParent(parent);
                if (pipelineBuilder != null) {
                    return pipelineBuilder;
                }
            }
        }
        return null;
    }

    protected PipelineBuilder createPipelineBuilder() {
        PipelineBuilder pipelineBuilder = new PipelineBuilder(this.pipelineBuilder.getPipelineNameSpace(), this.pipelineBuilder.getPipelineName());
        pipelineBuilder.setParentTableName(this.pipelineBuilder.getParentTableName());
        pipelineBuilder.setRootTableName(this.pipelineBuilder.getRootTableName());
        return pipelineBuilder;
    }

    /**
     * 合并一段sql到主sqlbuilder
     *
     * @param sqlBuilderResult
     */
    protected void mergeSQLBuilderResult(SqlBuilderResult sqlBuilderResult) {
        mergeSQLBuilderResult(sqlBuilderResult, false);
    }

    /**
     * 合并一段sql到主sqlbuilder
     *
     * @param sqlBuilderResult
     */
    protected void mergeSQLBuilderResult(SqlBuilderResult sqlBuilderResult, boolean isMergeGroup) {
        if (sqlBuilderResult == null) {
            return;
        }
        List<IConfigurable> configurableList = sqlBuilderResult.getConfigurables();
        if (sqlBuilderResult.isRightJoin()) {
            pipelineBuilder.setRightJoin(true);
        }
        if (CollectionUtil.isNotEmpty(configurableList)) {
            pipelineBuilder.addConfigurables(configurableList);
        }
        if (CollectionUtil.isNotEmpty(sqlBuilderResult.getStages())) {
            pipelineBuilder.getPipeline().getStages().addAll(sqlBuilderResult.getStages());
        }
        if (sqlBuilderResult.getFirstStage() != null) {
            pipelineBuilder.setHorizontalStages(sqlBuilderResult.getFirstStage());
        }
        if (sqlBuilderResult.getLastStage() != null) {
            pipelineBuilder.setCurrentChainStage(sqlBuilderResult.getLastStage());
        }
        if (pipelineBuilder.getCurrentStageGroup() == null && pipelineBuilder.getParentStageGroup() == null) {
            pipelineBuilder.setCurrentStageGroup(sqlBuilderResult.getStageGroup());

        } else {
            if (pipelineBuilder.getParentStageGroup() != null) {
                StageGroup parent = pipelineBuilder.getParentStageGroup();
                sqlBuilderResult.getStageGroup().setParent(parent);
                pipelineBuilder.setCurrentStageGroup(parent);
            } else {
                StageGroup children = pipelineBuilder.getCurrentStageGroup();
                StageGroup parent = sqlBuilderResult.getStageGroup();

                if (isMergeGroup && parent != null) {
                    children.setEndStage(parent.getEndStage());
                    children.setAllStageLables(parent.getAllStageLables());
                } else {
                    if (parent != null) {
                        children.setParent(parent);
                        pipelineBuilder.setCurrentStageGroup(parent);
                    }

                }

            }

        }

    }

    protected abstract void build();

    public String getFieldName(String fieldName) {
        String name = doAllFieldName(fieldName);
        if (name != null) {
            return name;
        }
        return getFieldName(asName, fieldName);
    }

    /**
     * 获取字段名，对于主流表，会去掉表名前缀，对于维度表，会加上表别名前缀。这里默认是主表逻辑，维度表会覆盖这个方法
     *
     * @param fieldName
     * @return
     */
    protected String getFieldName(String asName, String fieldName) {

        int index = fieldName.indexOf(".");
        if (index == -1) {
            return fieldName;
        }
        String ailasName = fieldName.substring(0, index);
        if (ailasName.equals(asName)) {
            return fieldName.substring(index + 1);
        } else {
            return null;
        }
    }

    public abstract String getFieldName(String fieldName, boolean containsSelf);

    /**
     * 获取字段名，对于主流表，会去掉表名前缀，对于维度表，会加上表别名前缀。这里默认是主表逻辑，维度表会覆盖这个方法
     *
     * @param fieldName
     * @return
     */
    protected String doAllFieldName(String fieldName) {
        if (fieldName.contains("*")) {
            int i = fieldName.indexOf(".");
            if (i != -1) {
                String asName = fieldName.substring(0, i);
                if (asName.equals(getAsName())) {
                    return fieldName;
                } else {
                    return null;
                }
            } else {
                return fieldName;
            }
        }
        return null;
    }

    public String getAsName() {
        return asName;
    }

    public void setAsName(String asName) {
        this.asName = asName;
    }

    public PipelineBuilder getPipelineBuilder() {
        return pipelineBuilder;
    }

    @Override
    public void setPipelineBuilder(PipelineBuilder pipelineBuilder) {
        this.pipelineBuilder = pipelineBuilder;
    }

    public List<AbstractSqlBuilder<?>> getParents() {
        return parents;
    }

    public void setParents(List<AbstractSqlBuilder<?>> parents) {
        this.parents = parents;
    }

    public boolean isSupportOptimization() {
        return supportOptimization;
    }

    public void setSupportOptimization(boolean supportOptimization) {
        this.supportOptimization = supportOptimization;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setCreateTable(String createTable) {
        this.createTable = createTable;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public Set<String> getDependentTables() {
        return dependentTables;
    }

    public Map<String, CreateSqlBuilder> getTableName2Builders() {
        return tableName2Builders;
    }

    public void setTableName2Builders(Map<String, CreateSqlBuilder> tableName2Builders) {
        this.tableName2Builders = tableName2Builders;
    }

    public void addScript(String scriptValue) {
        if (scriptValue == null) {
            return;
        }
        this.scripts.add(scriptValue);
    }

    public void setScripts(List<String> scripts) {
        this.scripts = scripts;
    }

    public List<String> getScripts() {
        return scripts;
    }

    public HashSet<String> getRootTableNames() {
        return rootTableNames;
    }

    public void setRootTableNames(HashSet<String> rootTableNames) {
        this.rootTableNames = rootTableNames;
    }

    public void addRootTableName(String rootTableName) {
        if (!"".equalsIgnoreCase(rootTableName) && rootTableName != null) {
            this.rootTableNames.add(rootTableName);
        }
    }

    public void addRootTableName(HashSet<String> rootTableNames) {
        if (rootTableNames != null && rootTableNames.size() > 0) {
            this.rootTableNames.addAll(rootTableNames);
        }
    }

    public String getSelectSql() {
        return selectSql;
    }

    public void setSelectSql(String selectSql) {
        this.selectSql = selectSql;
    }

    public String getFromSql() {
        return fromSql;
    }

    public void setFromSql(String fromSql) {
        this.fromSql = fromSql;
    }

    public String getWhereSql() {
        return whereSql;
    }

    public void setWhereSql(String whereSql) {
        this.whereSql = whereSql;
    }

    public String getGroupSql() {
        return groupSql;
    }

    public void setGroupSql(String groupSql) {
        this.groupSql = groupSql;
    }

    public String getHavingSql() {
        return havingSql;
    }

    public void setHavingSql(String havingSql) {
        this.havingSql = havingSql;
    }

    public String getJoinConditionSql() {
        return joinConditionSql;
    }

    public void setJoinConditionSql(String joinConditionSql) {
        this.joinConditionSql = joinConditionSql;
    }

    public List<String> getExpressionFunctionSql() {
        return expressionFunctionSql;
    }

    public void setExpressionFunctionSql(List<String> expressionFunctionSql) {
        this.expressionFunctionSql = expressionFunctionSql;
    }
}
