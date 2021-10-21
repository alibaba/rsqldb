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

import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class AbstractSQLBuilder<T extends AbstractSQLBuilder> implements ISQLBuilder {

    protected SqlNode sqlNode;//解析节点对应的sqlnode
    protected String sqlType;//sql的类型，是查询还是create
    protected List<T> children = new ArrayList<>();
    protected List<T> parents = new ArrayList<>();
    protected boolean supportOptimization = true;//是否支持优化，可以在不支持解析的地方设置这个参数，builder会忽略对这个节点的优化
    protected String tableName;//表名
    protected String createTable;//这个节点产生的新表
    protected String namespace;//命名空间
    //protected AbstractSQLBuilder treeSQLBulider;//在嵌套的场景，最外层的builder
    protected PipelineBuilder pipelineBuilder;
    protected Set<String> dependentTables = new HashSet<>();//对于上层table的依赖
    protected String asName;//表的别名，通过as修饰
    protected List<String> scripts = new ArrayList<>();//select，where部分有函数的，则把函数转换成脚本
    protected Map<String, CreateSQLBuilder> tableName2Builders = new HashMap<>();//保存所有create对应的builder， 在insert或维表join时使用
    protected HashSet<String> rootTableNames = new HashSet<>();

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

    @Override
    public String createSQL() {
        if (sqlNode == null) {
            return null;
        }
        return sqlNode.toString();
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

    public List<T> getChildren() {
        return children;
    }

    public void setChildren(List<T> children) {
        this.children = children;
    }

    public void addChild(T child) {
        this.children.add(child);
        child.getParents().add(this);
    }

    @Override
    public String getCreateTable() {
        return createTable;
    }

    @Override
    public void buildSQL() {
        if (getPipelineBuilder() == null) {
            PipelineBuilder pipelineBuilder = findPipelineBuilderFromParent(this);
            setPipelineBuilder(pipelineBuilder);
        }
        if (getPipelineBuilder() == null) {
            pipelineBuilder = new PipelineBuilder(getNamespace(), getTableName());
        }

        build();
    }

    protected PipelineBuilder findPipelineBuilderFromParent(AbstractSQLBuilder<T> sqlBuilder) {
        for (T parent : sqlBuilder.getParents()) {
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

    public List<T> getParents() {
        return parents;
    }

    public void setParents(List<T> parents) {
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

    public Map<String, CreateSQLBuilder> getTableName2Builders() {
        return tableName2Builders;
    }

    public void setTableName2Builders(
        Map<String, CreateSQLBuilder> tableName2Builders) {
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
}
