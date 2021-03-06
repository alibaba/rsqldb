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

import com.alibaba.rsqldb.parser.parser.SQLBuilderResult;
import com.alibaba.rsqldb.parser.parser.SQLParserContext;
import com.alibaba.rsqldb.parser.parser.result.IParseResult;
import com.alibaba.rsqldb.parser.parser.result.ScriptParseResult;
import com.alibaba.rsqldb.parser.parser.sqlnode.SelectParser;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlSelect;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.model.NameCreatorContext;
import org.apache.rocketmq.streams.common.topology.ChainStage;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.PrintUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.filter.operator.FilterOperator;
import org.apache.rocketmq.streams.script.operator.impl.ScriptOperator;

/**
 * ???????????????build???????????????????????????????????????????????????????????????????????????select???where??????
 */
public class SelectSQLBuilder extends AbstractSQLBuilder<AbstractSQLBuilder> {
    private static final Log LOG = LogFactory.getLog(SelectParser.class);

    /**
     * where????????????????????????????????????????????????(varName,functionName,value)&((varName,functionName,value)|(varName,functionName,int,value))
     */
    protected String expression;
    /**
     * ?????????????????????????????????????????????????????????????????????sub select  ?????????????????????
     */
    protected Set<String> allFieldNames = null;
    /**
     * ???sql select?????????????????????,???????????????????????????????????????????????????????????????
     */
    protected Map<String, IParseResult> fieldName2ParseResult = new HashMap<>();
    /**
     * field name order by sql select,use in insert table(field,field) select
     */
    protected List<String> fieldNamesOrderByDeclare = new ArrayList<>();
    /**
     * 0:select,1:from,2:where
     */
    protected int parseStage = 0;

    protected List<String> selectScripts = new ArrayList<>();
    /**
     * select????????????join?????????
     */
    protected JoinSQLBuilder joinSQLBuilder;
    /**
     * ?????????????????????????????????????????????
     */
    protected SelectSQLBuilder parentSelect;
    /**
     * ??????????????????????????????????????????
     */
    protected SelectSQLBuilder subSelect;
    protected boolean isDistinct = false;
    /**
     * group by ????????????
     */
    protected WindowBuilder windowBuilder;
    protected String overName;
    /**
     * ???union?????????
     */
    protected UnionSQLBuilder unionSQLBuilder;
    /**
     * ?????????lower???trim?????????????????????????????????????????????????????????????????????????????????????????????????????? string??????????????????;?????????
     */
    protected Map<String, String> innerVarNames = new HashMap();

    /**
     * use in create table
     */
    protected boolean closeFieldCheck = false;

    @Override
    public SQLBuilderResult buildSql() {
        if (pipelineBuilder == null) {
            pipelineBuilder = new PipelineBuilder(getSourceTable(), getSourceTable());
        }

        /**
         * ????????????select??????????????????join???????????????????????????????????????
         */
        if (joinSQLBuilder != null) {
            PipelineBuilder joinPipelineBuilder=createPipelineBuilder();
            joinSQLBuilder.setPipelineBuilder(joinPipelineBuilder);
            // joinSQLBuilder.setTreeSQLBulider(getTreeSQLBulider());
            joinSQLBuilder.setTableName2Builders(getTableName2Builders());
            joinSQLBuilder.setParentSelect(this);
            joinSQLBuilder.addRootTableName(this.getRootTableNames());
            SQLBuilderResult sqlBuilderResult=joinSQLBuilder.buildSql();
            mergeSQLBuilderResult(sqlBuilderResult);
            // buildSelect();
        }

        if (unionSQLBuilder != null) {
            if (!unionSQLBuilder.getTableName().equals(pipelineBuilder.getParentTableName())) {
                if (unionSQLBuilder.containsTableName(pipelineBuilder.getParentTableName())) {
                    unionSQLBuilder.setTableName(pipelineBuilder.getParentTableName());
                    this.setTableName(unionSQLBuilder.getTableName());
                    SelectSQLBuilder parent = this.parentSelect;
                    while (parent != null) {
                        parent.setTableName(this.getTableName());
                        parent = parent.parentSelect;
                    }
                }
                unionSQLBuilder.setTableName(pipelineBuilder.getParentTableName());
            }
            PipelineBuilder unionPipelineBuilder=createPipelineBuilder();
            unionSQLBuilder.setPipelineBuilder(unionPipelineBuilder);
            // unionSQLBuilder.setTreeSQLBulider(getTreeSQLBulider());
            unionSQLBuilder.setTableName2Builders(getTableName2Builders());
            unionSQLBuilder.setParentSelect(this);
            unionSQLBuilder.addRootTableName(this.getRootTableNames());
            SQLBuilderResult sqlBuilderResult= unionSQLBuilder.buildSql();
            mergeSQLBuilderResult(sqlBuilderResult);
        }
        /**
         * ???build??????????????????
         */
        if (subSelect != null) {
            PipelineBuilder subPipelineBuilder=createPipelineBuilder();
            subSelect.setPipelineBuilder(subPipelineBuilder);
            // subSelect.setTreeSQLBulider(getTreeSQLBulider());
            subSelect.setTableName2Builders(getTableName2Builders());
            subSelect.addRootTableName(this.getRootTableNames());
            SQLBuilderResult sqlBuilderResult=subSelect.buildSql();
            mergeSQLBuilderResult(sqlBuilderResult);
        }
        PipelineBuilder subPipelineBuilder=createPipelineBuilder();
        buildScript(subPipelineBuilder,getScripts(),false);
        bulidExpression(subPipelineBuilder);
        buildGroup(subPipelineBuilder);
        buildScript(subPipelineBuilder,getSelectScripts(),true);
        buildSelect(subPipelineBuilder);
        SQLBuilderResult sqlBuilderResult=new SQLBuilderResult(subPipelineBuilder,this);
        mergeSQLBuilderResult(sqlBuilderResult);
        ChainStage<?> first=CollectionUtil.isNotEmpty(pipelineBuilder.getFirstStages())?pipelineBuilder.getFirstStages().get(0):null;
        SQLBuilderResult result= new SQLBuilderResult(pipelineBuilder,first,pipelineBuilder.getCurrentChainStage());
        result.getStageGroup().setSql(this.createSQLFromParser());
        result.getStageGroup().setViewName(getFromSQL());
        return result;
    }



    /**
     * ????????????????????????????????????????????????pipline???stage
     */
    protected void buildScript(PipelineBuilder pipelineBuilder, List<String> scripts,boolean isSelect) {
        String script = createScript(scripts);
        if (script != null) {
           ChainStage chainStage= pipelineBuilder.addChainStage(new ScriptOperator(script));
           if(isSelect){
               chainStage.setSql(selectSQL);
               chainStage.setDiscription("SELECT Function Parser");
           }else {
               StringBuilder stringBuilder=new StringBuilder();
               for(String sql:this.getExpressionFunctionSQL()){
                   stringBuilder.append(sql+";"+PrintUtil.LINE);
               }
               chainStage.setSql(stringBuilder.toString());
               chainStage.setDiscription("Where Function Parser");
           }

        }
    }

    /**
     * ???????????????????????????????????????????????????
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
     * ??????where????????????????????????
     */
    protected void bulidExpression(PipelineBuilder pipelineBuilder) {
        if (StringUtil.isNotEmpty(expression)) {
            SqlSelect sqlSelect = (SqlSelect) sqlNode;
            sqlSelect.setWhere(null);
            String ruleName = NameCreatorContext.get().createOrGet(this.getPipelineBuilder().getPipelineName()).createName(this.getPipelineBuilder().getPipelineName(), "rule");
            ChainStage chainStage=pipelineBuilder.addChainStage(new FilterOperator(getNamespace(), ruleName, expression));
            chainStage.setSql(whereSQL);
            chainStage.setDiscription("Where Expression");
        }

    }

    /**
     * build group?????????window??????
     */
    protected void buildGroup(PipelineBuilder pipelineBuilder) {
        if (windowBuilder != null) {
            windowBuilder.setPipelineBuilder(pipelineBuilder);
            //windowBuilder.setTreeSQLBulider(getTreeSQLBulider());
            windowBuilder.setOwner(this);
            windowBuilder.build();
        }
    }

    /**
     * ???select?????????????????????????????????????????????????????????????????????scripts?????????????????? ????????????select ???????????????????????????????????????
     */
    protected void buildSelect(PipelineBuilder pipelineBuilder) {

        StringBuilder stringBuilder = new StringBuilder();
        Set<String> allFieldNames = new HashSet<>();
        /**
         * ??????????????? retain?????????????????????????????????????????????????????????scripts????????????????????????????????????????????????????????????
         */
        String retainScript = "retainField(";
        boolean isFirst = true;
        String[] fieldNames = new String[fieldName2ParseResult.size()];
        int i = 0;
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
                    String fieldName = doAsteriskTrimAliasName(entry.getKey(), stringBuilder, allFieldNames);
                    retainScript += fieldName;
                    fieldNames[i] = fieldName;
                } else {
                    String name = entry.getKey();
                    if (name.indexOf(".") != -1) {
                        int index = name.indexOf(".");
                        name = name.substring(index + 1);

                        stringBuilder.append(name + "=" + entry.getKey() + ";" + PrintUtil.LINE);

                    }
                    allFieldNames.add(name);
                    retainScript += name;
                    fieldNames[i] = name;
                }
                IParseResult parseResult = entry.getValue();

                if (ScriptParseResult.class.isInstance(parseResult)) {
                    ScriptParseResult scriptParseResult = (ScriptParseResult) parseResult;
                    List<String> scripts = scriptParseResult.getScriptValueList();
                    if (scripts != null) {
                        for (String script : scripts) {
                            /**
                             * ????????????scripts?????? ?????????????????????
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
            if (isDistinct) {
                String distinctScript = scriptValue.replace("retainField", "distinct");
                scriptValue += distinctScript;
            }
            ChainStage chainStage=pipelineBuilder.addChainStage(new ScriptOperator(scriptValue));
            chainStage.setSql(selectSQL);
            chainStage.setDiscription("SELECT Fields");
            //optimizer.put(scriptValue,true);
        }

    }

    /**
     * ??????????????????????????????????????????*?????????fieldName2ParseResult ??????????????????*??????????????????????????????
     *
     * @return
     */
    public Set<String> getAllFieldNames() {

        //?????????????????????????????????????????????allFieldNames?????????????????????????????????????????????????????????????????????????????????????????????allFieldNames????????????
        if (allFieldNames != null && allFieldNames.size() > 0) {
            return allFieldNames;
        }
        Set<String> fieldNames = new HashSet<>();
        //??????select ????????????????????????*???????????????
        if (fieldName2ParseResult != null) {
            Iterator<Entry<String, IParseResult>> it = fieldName2ParseResult.entrySet().iterator();

            while (it.hasNext()) {
                Entry<String, IParseResult> entry = it.next();
                String fieldName = entry.getKey();
                if (entry.getKey().indexOf("*") != -1) {
                    //???*???????????????????????????
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
     * ?????????*????????????????????????????????????
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
        //?????????????????????????????????????????????a.*
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
            //??????????????????????????????????????????????????????????????????????????????
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
     * ?????????*????????????????????????????????????
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
     * ?????????*????????????????????????????????????
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
             * ?????????select?????? b.c as c,???*??????b.c??????????????????
             */
            if (inSelectField(fieldName)) {
                continue;
            }
            int index = fieldName.indexOf(".");
            if (index != -1) {
                String name = fieldName.substring(index + 1);
               // if (fieldNames.contains(name)) {
                    stringBuilder.append(name).append("=").append(fieldName).append(";").append(PrintUtil.LINE);
                    list.add(name);
                //}
            } else {
                list.add(fieldName);
            }
        }
        allFieldNames.addAll(list);
        return MapKeyUtil.createKey(",", list);
    }

    /**
     * ??????*?????????????????????select?????? ???as ??????????????????true
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
            if (entry.getValue() instanceof ScriptParseResult) {
                ScriptParseResult scriptParseResult = (ScriptParseResult) entry.getValue();
                value = scriptParseResult.getScript();
                if (value.endsWith("=" + fieldName + ";")) {
                    return true;
                }
            } else {
                value = entry.getValue().getReturnValue();
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
        UnionSQLBuilder union = this.unionSQLBuilder;
        if (union != null) {
            dependentTables.addAll(union.parseDependentTables());
        }
        return dependentTables;
    }

    @Override
    public String createSql() {
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
     * ??????????????????????????????????????????????????????????????????????????????
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
     * ???????????????????????????sql????????????sql????????????where???from?????????????????????????????????????????????????????????????????????join???????????????????????????????????????select???????????????
     *
     * @param fieldName
     * @param containsSelf
     * @return
     */
    @Override
    public String getFieldName(String fieldName, boolean containsSelf) {
        /**
         * ??????????????????*??????????????????????????????
         */
        String value = doAllFieldName(fieldName);
        if (value != null) {
            return value;
        }

        /**
         * ??????????????????,?????????????????????????????????
         */
        if (containsSelf) {
            value = findFieldBySelect(this, fieldName);
            if (value != null) {
                return value;
            }
        }

        /**
         *?????????join????????????join?????????
         */
        if (joinSQLBuilder != null) {
            value = joinSQLBuilder.getFieldName(fieldName);
            if (StringUtil.isNotEmpty(value)) {
                return value;
            }
        }

        /**
         * ?????????union?????????unoin?????????
         */
        if (unionSQLBuilder != null) {
            value = unionSQLBuilder.getFieldName(fieldName);
            if (StringUtil.isNotEmpty(value)) {
                return value;
            }
        }

        /**
         * ?????????window ?????????window?????????
         */
        if (windowBuilder != null) {
            value = windowBuilder.getFieldName(fieldName);
            if (StringUtil.isNotEmpty(value)) {
                return value;
            }
        }

        /**
         * ????????????
         */
        if (subSelect != null) {
            value = findFieldBySelect(subSelect, fieldName);
            if (value != null) {
                return value;
            }
        }

        /**
         * ????????????????????????
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
                String tableAilasName = getAsName();
                if (ailasName != null && tableAilasName == null) {
                    tableAilasName = getTableName();
                }
                if (ailasName.equals(tableAilasName)) {
                    if (fieldNames.contains(fieldName)) {
                        return fieldName;
                    }
                }
            }
        }
        return null;
    }

    /**
     * ?????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
     *
     * @param fieldName
     * @return
     */
    @Override
    @Deprecated
    public String getFieldName(String fieldName) {
        if (isCloseFieldCheck()) {
            return fieldName;
        }
        return getFieldName(fieldName, false);
    }

    /**
     * ????????????select??????????????????
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
     * ???????????????????????????
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
     * ????????????????????????????????????sql????????????????????????????????????????????????????????????????????????????????? ????????????trim???lower??????
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
