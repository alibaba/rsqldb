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

import org.apache.rocketmq.streams.filter.operator.FilterOperator;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.PrintUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.operator.impl.ScriptOperator;
import com.alibaba.rsqldb.parser.parser.SQLParserContext;
import com.alibaba.rsqldb.parser.parser.result.IParseResult;
import com.alibaba.rsqldb.parser.parser.result.ScriptParseResult;
import com.alibaba.rsqldb.parser.parser.sqlnode.SelectParser;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlSelect;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * 这块是整个build的核心，因为所有的优化都是基于过滤的，过滤都体现在select的where部分
 */
public class SelectSQLBuilder extends AbstractSQLBuilder<AbstractSQLBuilder> {
    private static final Log LOG = LogFactory.getLog(SelectParser.class);

    protected String expression;//where部分逻辑判断的转换成表达式字符串(varName,functionName,value)&((varName,functionName,value)|(varName,functionName,int,value))

    protected Set<String> allFieldNames = null;//如果做过字段解析，把所有的字段放到这里面，在做sub select  取值时，直接取

    /**
     * 在sql select部分出现的字段
     */
    protected Map<String, IParseResult> fieldName2ParseResult = new HashMap<>();//每个字段和对应解析器，在必要的时候完成解析
    protected List<String> fieldNamesOrderByDeclare=new ArrayList<>();//field name order by sql select,use in insert table(field,field) select
    protected int parseStage = 0;//0:select,1:from,2:where

    protected List<String> selectScripts = new ArrayList<>();

    protected JoinSQLBuilder joinSQLBuilder;// select场景中有join的场景
    protected SelectSQLBuilder parentSelect;//如果有嵌套查询，则表示外侧查询
    protected SelectSQLBuilder subSelect;//如果有嵌套查询，则表示子查询
    protected boolean isDistinct=false;
    /**
     * group by 相关参数
     */
    protected WindowBuilder windowBuilder;
    protected String overName;

    //有union的场景
    protected UnionSQLBuilder unionSQLBuilder;

    //主要是lower或trim等函数，如果同一字段添加多次函数，最终只返回一个名字。使用时需要谨慎
    //string：包含函数名;变量名
    protected Map<String, String> innerVarNames = new HashMap();


    protected boolean closeFieldCheck=false;//use in create table
    @Override
    public void buildSQL() {
        if (pipelineBuilder == null) {
            pipelineBuilder = new PipelineBuilder(getSourceTable(), getSourceTable());
        }

        /**
         * 无论这个select是否能优化，join查询也需要判断是否可以优化
         */
        if (joinSQLBuilder != null) {
            joinSQLBuilder.setPipelineBuilder(pipelineBuilder);
            // joinSQLBuilder.setTreeSQLBulider(getTreeSQLBulider());
            joinSQLBuilder.setTableName2Builders(getTableName2Builders());
            joinSQLBuilder.setParentSelect(this);
            joinSQLBuilder.addRootTableName(this.getRootTableNames());
            joinSQLBuilder.build();
            // buildSelect();
        }

        if (unionSQLBuilder != null) {
            if(!unionSQLBuilder.getTableName().equals(pipelineBuilder.getParentTableName())){
                if(unionSQLBuilder.containsTableName(pipelineBuilder.getParentTableName())){
                    unionSQLBuilder.setTableName(pipelineBuilder.getParentTableName());
                    this.setTableName(unionSQLBuilder.getTableName());
                    SelectSQLBuilder parent=this.parentSelect;
                    while (parent!=null){
                        parent.setTableName(this.getTableName());
                        parent=parent.parentSelect;
                    }
                }
                unionSQLBuilder.setTableName(pipelineBuilder.getParentTableName());
            }
            unionSQLBuilder.setPipelineBuilder(pipelineBuilder);
            // unionSQLBuilder.setTreeSQLBulider(getTreeSQLBulider());
            unionSQLBuilder.setTableName2Builders(getTableName2Builders());
            unionSQLBuilder.setParentSelect(this);
            unionSQLBuilder.addRootTableName(this.getRootTableNames());
            unionSQLBuilder.buildSQL();
        }
        /**
         * 先build嵌套的子查询
         */
        if (subSelect != null) {
            subSelect.setPipelineBuilder(pipelineBuilder);
            // subSelect.setTreeSQLBulider(getTreeSQLBulider());
            subSelect.setTableName2Builders(getTableName2Builders());
            subSelect.addRootTableName(this.getRootTableNames());
            subSelect.buildSQL();
        }

        buildScript(getScripts());
        bulidExpression();
        buildGroup();
        buildScript(getSelectScripts());
        buildSelect();
    }

    /**
     * 把解析字段过程中生成的脚本，生成pipline的stage
     */
    protected void buildScript(List<String> scripts) {
        String script = createScript(scripts);
        if (script != null) {
            pipelineBuilder.addChainStage(new ScriptOperator(script));
        }
    }

    /**
     * 把多个脚本拼接成一个脚本，用；分割
     *
     * @param scripts
     * @return
     */
    public String createScript(List<String> scripts) {
        StringBuilder stringBuilder = new StringBuilder();
        if (scripts != null && scripts.size() > 0) {
            for (String script : scripts) {
                stringBuilder.append(script + PrintUtil.LINE);
                if (!script.endsWith(";")) {
                    stringBuilder.append(";");
                }
            }
            return stringBuilder.toString();
        }
        return null;
    }

    /**
     * 解析where部分生成的表达式
     */
    protected void bulidExpression() {
        if (StringUtil.isNotEmpty(expression)) {
            SqlSelect sqlSelect = (SqlSelect)sqlNode;
            sqlSelect.setWhere(null);
            pipelineBuilder.addChainStage(new FilterOperator(expression));
        }

    }

    /**
     * build group，创建window对象
     */
    protected void buildGroup() {
        if (windowBuilder != null) {
            windowBuilder.setPipelineBuilder(pipelineBuilder);
            //  windowBuilder.setTreeSQLBulider(getTreeSQLBulider());
            windowBuilder.setOwner(this);
            windowBuilder.build();
        }
    }

    /**
     * 把select部分的字段做处理，如果解析时生成了脚本，且未在scripts中，生成脚本 同时根据select 字段列表，去除掉无用的字段
     */
    protected void buildSelect() {

        StringBuilder stringBuilder = new StringBuilder();
        Set<String> allFieldNames = new HashSet<>();
        /**
         * 把字段生成 retain函数，因为字段解析中多数脚本已经放到了scripts成员变量中，如果个别没有，也需要生成脚本
         */
        String retainScript = "retainField(";
        boolean isFirst = true;
        String[] fieldNames=new String[fieldName2ParseResult.size()];
        int i=0;
        if (fieldName2ParseResult != null) {
            Iterator<Entry<String, IParseResult>> it = fieldName2ParseResult.entrySet().iterator();

            while (it.hasNext()) {
                Entry<String, IParseResult> entry = it.next();
                if (isFirst) {
                    isFirst = false;

                } else {
                    retainScript += ",";
                }
                if (entry.getKey().indexOf("*") != -1) {
                    String fieldName=doAsteriskTrimAliasName(entry.getKey(), stringBuilder, allFieldNames);
                    retainScript += fieldName;
                    fieldNames[i]=fieldName;
                } else {
                    String name = entry.getKey();
                    if (name.indexOf(".") != -1) {
                        int index = name.indexOf(".");
                        name = name.substring(index + 1);

                        stringBuilder.append(name + "=" + entry.getKey() + ";" + PrintUtil.LINE);

                    }
                    allFieldNames.add(name);
                    retainScript += name;
                    fieldNames[i]=name;
                }
                IParseResult parseResult = entry.getValue();

                if (ScriptParseResult.class.isInstance(parseResult)) {
                    ScriptParseResult scriptParseResult = (ScriptParseResult)parseResult;
                    List<String> scripts = scriptParseResult.getScriptValueList();
                    if (scripts != null) {
                        for (String script : scripts) {
                            /**
                             * 去重，在scripts中的 已经做过处理了
                             */
                            if (!scripts.contains(script)) {
                                stringBuilder.append(script + PrintUtil.LINE);
                                if (!script.endsWith(";")) {
                                    stringBuilder.append(";");
                                }
                            }
                        }
                    }
                }
                i++;

            }
            retainScript += ");";

        }

        stringBuilder.append(retainScript);
        String scriptValue = stringBuilder.toString();
        this.allFieldNames = allFieldNames;
        if (isFirst == false) {
            if(isDistinct){
                String distinctScript=scriptValue.replace("retainField","distinct");
                scriptValue+=distinctScript;
            }
            pipelineBuilder.addChainStage(new ScriptOperator(scriptValue));
            //optimizer.put(scriptValue,true);
        }

    }

    /**
     * 获取所有字段名，需要特殊处理*号。从fieldName2ParseResult 中找，如果有*号，则需要进一步展开
     *
     * @return
     */
    public Set<String> getAllFieldNames() {

        //如果做过解析，会缓存解析结果在allFieldNames，直接返回（主要是嵌套场景），否则需要解析，把所有的字段缓存在allFieldNames这个字段
        if (allFieldNames != null && allFieldNames.size() > 0) {
            return allFieldNames;
        }
        Set<String> fieldNames = new HashSet<>();
        //解析select 中的所有字段，对*号特殊处理
        if (fieldName2ParseResult != null) {
            Iterator<Entry<String, IParseResult>> it = fieldName2ParseResult.entrySet().iterator();

            while (it.hasNext()) {
                Entry<String, IParseResult> entry = it.next();
                String fieldName = entry.getKey();
                if (entry.getKey().indexOf("*") != -1) {
                    //把*号解析成具体字段值
                    Set<String> names = doAsterisk2Set(fieldName);
                    if (names != null) {
                        fieldNames.addAll(names);
                    }

                } else {
                    fieldNames.add(fieldName);
                }

            }
        }
        return fieldNames;
    }

    /**
     * 如果有*号，则展开对应的所有字段
     *
     * @param key
     * @return
     */
    protected Set<String> doAsterisk2Set(String key) {
        int index = key.indexOf("*");
        if (index == -1) {
            return null;
        }
        String aliasName = null;
        index = key.indexOf(".");
        //如果有别名，把别名抽取出来，如a.*
        if (index != -1) {
            aliasName = key.substring(0, index);
            key = "*";
        }

        if (aliasName == null || (aliasName != null && aliasName.equals(getAsName()))) {
            if (allFieldNames != null && allFieldNames.size() > 0) {
                return allFieldNames;
            }
        }

        Set<String> fieldNames = null;
        if (key.equals("*")) {
            //如果别名为空，或别名等于字表，递归调用字表的全部字段
            if (subSelect != null && (aliasName == null || (aliasName != null && aliasName.equals(subSelect.getAsName())))) {
                if (subSelect.allFieldNames != null && subSelect.allFieldNames.size() > 0) {
                    return subSelect.allFieldNames;
                } else {
                    fieldNames = subSelect.getAllFieldNames();
                }

            } else if (joinSQLBuilder != null) {
                if (aliasName == null) {
                    fieldNames = joinSQLBuilder.getAllFieldNames();
                } else {
                    fieldNames = joinSQLBuilder.getAllFieldNames(aliasName);
                }

            } else if (unionSQLBuilder != null) {
                fieldNames = unionSQLBuilder.getAllFieldNames();
            } else {
                fieldNames = SQLParserContext.getInstance().get().get(getTableName());
            }

        }
        return fieldNames;
    }
    /**
     * 如果有*号，则展开对应的所有字段
     * @param key
     * @return
     */
    //protected String doAsterisk(String key) {
    //    Set<String> fieldNames = doAsterisk2Set(key);
    //    if (fieldNames == null) {
    //        return null;
    //    }
    //    List<String> list = new ArrayList<>();
    //    list.addAll(fieldNames);
    //    return MapKeyUtil.createKey(",", list);
    //}

    /**
     * 如果有*号，则展开对应的所有字段
     *
     * @param key
     * @return
     */
    protected String doAsteriskTrimAliasName(String key, StringBuilder stringBuilder, Set<String> allFieldNames) {

        Set<String> fieldNames = doAsterisk2Set(key);
        if (fieldNames == null) {
            return null;
        }
        List<String> list = new ArrayList<>();
        for (String fieldName : fieldNames) {
            /**
             * 如果在select中有 b.c as c,则*中的b.c字段不再处理
             */
            if (inSelectField(fieldName)) {
                continue;
            }
            int index = fieldName.indexOf(".");
            if (index != -1) {
                String name = fieldName.substring(index + 1);
                if (fieldNames.contains(name)) {
                    stringBuilder.append(name + "=" + fieldName + ";" + PrintUtil.LINE);
                    list.add(name);
                }
            } else {
                list.add(fieldName);
            }
        }
        allFieldNames.addAll(list);
        return MapKeyUtil.createKey(",", list);
    }

    /**
     * 如果*里面的字段，在select中有 做as 处理，则返回true
     *
     * @param fieldName
     * @return
     */
    protected boolean inSelectField(String fieldName) {
        Iterator<Entry<String, IParseResult>> it = this.fieldName2ParseResult.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, IParseResult> entry = it.next();
            String key = entry.getKey();
            String value = null;
            if (ScriptParseResult.class.isInstance(entry.getValue())) {
                ScriptParseResult scriptParseResult = (ScriptParseResult)entry.getValue();
                value = scriptParseResult.getScript();
                if (value.endsWith("=" + fieldName + ";")) {
                    return true;
                }
            } else {
                value = entry.getValue().getReturnValue();
                if (key.equals(value)) {
                    continue;
                }
            }

        }
        return false;
    }

    @Override
    protected void build() {

    }

    @Override
    public Set<String> parseDependentTables() {
        Set<String> dependentTables = new HashSet<>();
        if (this.getDependentTables() != null && this.getDependentTables().size() > 0) {
            dependentTables.addAll(this.getDependentTables());
        }

        SelectSQLBuilder current = this.subSelect;
        while (current != null) {
            dependentTables.addAll(current.parseDependentTables());
            current = current.subSelect;
        }
        SelectSQLBuilder join = this.joinSQLBuilder;
        if (join != null) {
            dependentTables.addAll(join.parseDependentTables());
        }
        UnionSQLBuilder union=this.unionSQLBuilder;
        if(union!=null){
            dependentTables.addAll(union.parseDependentTables());
        }
        return dependentTables;
    }

    @Override
    public String createSQL() {
        return sqlNode.toSqlString(SqlDialect.DatabaseProduct.HSQLDB.getDialect()).toString();
    }

    public void switchSelect() {
        this.parseStage = 0;
    }

    public void switchFrom() {
        this.parseStage = 1;
    }

    public void switchWhere() {
        this.parseStage = 2;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    public void putSelectField(String fieldName, IParseResult parseResult) {
        this.fieldNamesOrderByDeclare.add(fieldName);
        fieldName2ParseResult.put(fieldName, parseResult);
    }

    public boolean isSelectStage() {
        return (0 == parseStage);
    }

    public boolean isFromStage() {
        return (1 == parseStage);
    }

    public boolean isWhereStage() {
        return (2 == parseStage);
    }

    public JoinSQLBuilder getJoinSQLDescriptor() {
        return joinSQLBuilder;
    }

    public void setJoinSQLDescriptor(JoinSQLBuilder joinSQLDescriptor) {
        this.joinSQLBuilder = joinSQLDescriptor;
    }

    /**
     * 获取最底层的表名，如果嵌套多层，最里层的应该是源头表
     *
     * @return
     */
    public String getSourceTable() {
        String tableName = getTableName();
        if (subSelect != null) {
            tableName = subSelect.getSourceTable();
        }
        return tableName;
    }

    /**
     * 确定字段是否在这个sql中，如果sql解析的是where，from部分，应该不包含自己的输出，如果自己是字表，或join的左右表，则应该包含自己的select部分的输出
     *
     * @param fieldName
     * @param containsSelf
     * @return
     */
    @Override
    public String getFieldName(String fieldName, boolean containsSelf) {
        /**
         * 如果字段包含*号，直接返回原有字段
         */
        String value = doAllFieldName(fieldName);
        if (value != null) {
            return value;
        }

        /**
         * 处理标准输出,标准输出，不带任何别名
         */
        if (containsSelf) {
            value = findFieldBySelect(this, fieldName);
            if (value != null) {
                return value;
            }
        }

        /**
         *如果是join，则委托join做处理
         */
        if (joinSQLBuilder != null) {
            value = joinSQLBuilder.getFieldName(fieldName);
            if (StringUtil.isNotEmpty(value)) {
                return value;
            }
        }

        /**
         * 如果是union，委托unoin做处理
         */
        if (unionSQLBuilder != null) {
            value = unionSQLBuilder.getFieldName(fieldName);
            if (StringUtil.isNotEmpty(value)) {
                return value;
            }
        }

        /**
         * 如果是window ，委托window做处理
         */
        if (windowBuilder != null) {
            value = windowBuilder.getFieldName(fieldName);
            if (StringUtil.isNotEmpty(value)) {
                return value;
            }
        }

        /**
         * 检查子表
         */
        if (subSelect != null) {
            value = findFieldBySelect(subSelect, fieldName);
            if (value != null) {
                return value;
            }
        }

        /**
         * 检查原始表的输出
         */
        Set<String> fieldNames = SQLParserContext.getInstance().get().get(getTableName());
        if (fieldNames != null) {
            int index = fieldName.indexOf(".");
            if (index == -1) {
                if (fieldNames.contains(fieldName)) {
                    return fieldName;
                }
            } else {
                String ailasName = fieldName.substring(0, index);
                fieldName = fieldName.substring(index + 1);
                if (ailasName.equals(getAsName())) {
                    if (fieldNames.contains(fieldName)) {
                        return fieldName;
                    }
                }
            }
        }
        return null;
    }

    /**
     * 获取字段名，对于主流表，会去掉表名前缀，对于维度表，会加上表别名前缀。这里默认是主表逻辑，维度表会覆盖这个方法
     *
     * @param fieldName
     * @return
     */
    @Override
    @Deprecated
    public String getFieldName(String fieldName) {
        if(isCloseFieldCheck()){
            return fieldName;
        }
        return getFieldName(fieldName, false);
    }

    /**
     * 从查询的select部分查找字段
     *
     * @param subSelect
     * @param fieldName
     * @return
     */
    public String findFieldBySelect(SelectSQLBuilder subSelect, String fieldName) {
        String asteriskName = subSelect.doAllFieldName(fieldName);
        if (asteriskName != null) {
            return asteriskName;
        }
        int index = fieldName.indexOf(".");
        if (index != -1) {
            fieldName = fieldName.substring(index + 1);
        }

        if (subSelect != null) {
            Set<String> keySet = subSelect.getAllFieldNames();
            for (String key : keySet) {
                String name = key;
                if (key.indexOf(".") != -1) {
                    name = key.substring(key.indexOf(".") + 1);
                }
                if (name.equals(fieldName)) {
                    return name;
                }
            }
        }
        return null;
    }

    /**
     * 从子查询，查找字段
     *
     * @param
     * @return
     */
    //protected String findBySubSelect(String fieldName) {
    //    SelectSQLBuilder subSelect = getSubSelect();
    //    return findFieldBySelect(subSelect,fieldName);
    //}
    public SelectSQLBuilder getParentSelect() {
        return parentSelect;
    }

    public void setParentSelect(SelectSQLBuilder parentSelect) {
        this.parentSelect = parentSelect;
    }

    public SelectSQLBuilder getSubSelect() {
        return subSelect;
    }

    public void setSubSelect(SelectSQLBuilder subSelect) {
        this.subSelect = subSelect;
    }

    public Map<String, IParseResult> getFieldName2ParseResult() {
        return fieldName2ParseResult;
    }

    public WindowBuilder getWindowBuilder() {
        return windowBuilder;
    }

    public List<String> getSelectScripts() {
        return selectScripts;
    }

    public void setSelectScripts(List<String> selectScripts) {
        this.selectScripts = selectScripts;
    }

    public UnionSQLBuilder getUnionSQLBuilder() {
        return unionSQLBuilder;
    }

    public void setUnionSQLBuilder(UnionSQLBuilder unionSQLBuilder) {
        this.unionSQLBuilder = unionSQLBuilder;
    }

    public void setWindowBuilder(WindowBuilder windowBuilder) {
        this.windowBuilder = windowBuilder;
    }

    /**
     * 对于内置变量，如果在一段sql，对一个变量做多次操作，可以通过这个方法变成只有一次。 主要用于trim，lower函数
     *
     * @param funcitonName
     * @param varName
     * @return
     */
    public String getInnerVarName(String funcitonName, String varName) {
        String key = MapKeyUtil.createKeyBySign(";", funcitonName, varName);
        if (innerVarNames.containsKey(key)) {
            return innerVarNames.get(key);
        }
        return null;
    }

    public void putInnerVarName(String funcitonName, String varName, String innerVarName) {
        String key = MapKeyUtil.createKeyBySign(";", funcitonName, varName);
        innerVarNames.put(key, innerVarName);
    }

    public boolean isDistinct() {
        return isDistinct;
    }

    public void setDistinct(boolean distinct) {
        isDistinct = distinct;
    }

    public String getOverName() {
        return overName;
    }

    public void setOverName(String overName) {
        this.overName = overName;
    }

    public int getParseStage() {
        return parseStage;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public void setParseStage(int parseStage) {
        this.parseStage = parseStage;
    }

    public boolean isCloseFieldCheck() {
        return closeFieldCheck;
    }

    public List<String> getFieldNamesOrderByDeclare() {
        return fieldNamesOrderByDeclare;
    }

    public void setCloseFieldCheck(boolean closeFieldCheck) {
        this.closeFieldCheck = closeFieldCheck;
    }
}
