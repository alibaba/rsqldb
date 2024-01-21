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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.alibaba.rsqldb.parser.SqlBuilderResult;
import com.alibaba.rsqldb.parser.result.IParseResult;
import com.alibaba.rsqldb.parser.result.ScriptParseResult;
import com.alibaba.rsqldb.parser.sql.SQLParseContext;
import com.alibaba.rsqldb.parser.sql.context.FieldsOfTableForSqlTreeContext;
import com.alibaba.rsqldb.parser.sqlnode.AbstractSelectParser;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlSelect;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.model.NameCreatorContext;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.common.topology.model.AbstractChainStage;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.PrintUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.filter.operator.FilterOperator;
import org.apache.rocketmq.streams.script.operator.impl.ScriptOperator;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

/**
 * 这块是整个build的核心，因为所有的优化都是基于过滤的，过滤都体现在select的where部分
 */
public class SelectSqlBuilder extends AbstractSqlBuilder {
    public static final String WINDOW_HITS_NAME = "WIN";
    public static final String HITS_WINDOW_SIZE = "window.size";//通过hits设置orderby的窗口大小，默认是1个小时
    public static final String HITS_WINDOW_BEFORE_EMIT = "window.before.emit";//通过hits设置orderby的窗口大小，默认是1个小时
    public static final String HITS_WINDOW_AFTER_EMIT = "window.after.emit";//通过hits设置orderby的窗口大小，默认是1个小时
    public static final String HITS_WINDOW_WATERMARK = "window.watermark";//通过hits设置orderby的窗口大小，默认是1个小时
    public static final String HITS_WINDOW_TIMEFIELD = "window.time.field";//通过hits设置orderby的窗口大小，默认是1个小时
    private static final Log LOG = LogFactory.getLog(AbstractSelectParser.class);
    /**
     * where部分逻辑判断的转换成表达式字符串(varName,functionName,value)&((varName,functionName,value)|(varName,functionName,int,value))
     */
    protected String expression;
    /**
     * 如果做过字段解析，把所有的字段放到这里面，在做sub select  取值时，直接取
     */
    protected Set<String> allFieldNames = null;
    /**
     * 在sql select部分出现的字段,每个字段和对应解析器，在必要的时候完成解析
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
     * select场景中有join的场景
     */
    protected JoinSqlBuilder joinSQLBuilder;

    protected Map<String, Map<String, String>> hits = new HashMap<>();
    /**
     * 如果有嵌套查询，则表示外侧查询
     */
    protected SelectSqlBuilder parentSelect;
    /**
     * 如果有嵌套查询，则表示子查询
     */
    protected SelectSqlBuilder subSelect;
    protected boolean isDistinct = false;
    /**
     * group by 相关参数
     */
    protected WindowBuilder windowBuilder;

    protected WindowBuilder overWindowBuilder;
    protected String overName;
    /**
     * 有union的场景
     */
    protected UnionSqlBuilder unionSQLBuilder;
    /**
     * 主要是lower或trim等函数，如果同一字段添加多次函数，最终只返回一个名字。使用时需要谨慎 string：包含函数名;变量名
     */
    protected Map<String, String> innerVarNames = new HashMap();

    /**
     * use in create table, not check field exist
     */
    protected boolean closeFieldCheck = false;

    private boolean isViewSink;//特殊处理：如果insert into 对应的是view sink，则不过滤输出字段

    @Override
    public SqlBuilderResult buildSql() {
        if (pipelineBuilder == null) {
            pipelineBuilder = new PipelineBuilder(getSourceTable(), getSourceTable(), this.configuration);
        }

        /**
         * 无论这个select是否能优化，join查询也需要判断是否可以优化
         */
        if (joinSQLBuilder != null) {
            PipelineBuilder joinPipelineBuilder = createPipelineBuilder();
            joinSQLBuilder.setPipelineBuilder(joinPipelineBuilder);
            joinSQLBuilder.setConfiguration(getConfiguration());
            // joinSQLBuilder.setTreeSQLBulider(getTreeSQLBulider());
            joinSQLBuilder.setTableName2Builders(getTableName2Builders());
            joinSQLBuilder.setParentSelect(this);
            joinSQLBuilder.addRootTableName(this.getRootTableNames());
            SqlBuilderResult sqlBuilderResult = joinSQLBuilder.buildSql();
            if (sqlBuilderResult.isRightJoin()) {
                return sqlBuilderResult;
            }
            mergeSQLBuilderResult(sqlBuilderResult);

            // buildSelect();
        }

        if (unionSQLBuilder != null) {
            unionSQLBuilder.setConfiguration(getConfiguration());
            if (!unionSQLBuilder.getTableName().equals(pipelineBuilder.getParentTableName())) {
                if (unionSQLBuilder.containsTableName(pipelineBuilder.getParentTableName())) {
                    unionSQLBuilder.setTableName(pipelineBuilder.getParentTableName());
                    this.setTableName(unionSQLBuilder.getTableName());
                    SelectSqlBuilder parent = this.parentSelect;
                    while (parent != null) {
                        parent.setTableName(this.getTableName());
                        parent = parent.parentSelect;
                    }
                }
                unionSQLBuilder.setTableName(pipelineBuilder.getParentTableName());
            }
            PipelineBuilder unionPipelineBuilder = createPipelineBuilder();
            unionSQLBuilder.setPipelineBuilder(unionPipelineBuilder);
            // unionSQLBuilder.setTreeSQLBulider(getTreeSQLBulider());
            unionSQLBuilder.setTableName2Builders(getTableName2Builders());
            unionSQLBuilder.setParentSelect(this);
            unionSQLBuilder.addRootTableName(this.getRootTableNames());
            SqlBuilderResult sqlBuilderResult = unionSQLBuilder.buildSql();
            mergeSQLBuilderResult(sqlBuilderResult);
        }
        /**
         * 先build嵌套的子查询
         */
        if (subSelect != null) {
            subSelect.setConfiguration(getConfiguration());
            PipelineBuilder subPipelineBuilder = createPipelineBuilder();
            subSelect.setPipelineBuilder(subPipelineBuilder);
            // subSelect.setTreeSQLBulider(getTreeSQLBulider());
            subSelect.setTableName2Builders(getTableName2Builders());
            subSelect.addRootTableName(this.getRootTableNames());
            SqlBuilderResult sqlBuilderResult = subSelect.buildSql();
            mergeSQLBuilderResult(sqlBuilderResult);
        }
        boolean isMergeGroup = false;
        PipelineBuilder subPipelineBuilder = createPipelineBuilder();

        buildScript(subPipelineBuilder, getScripts(), false);
        bulidExpression(subPipelineBuilder);
        SqlBuilderResult sqlBuilderResult = new SqlBuilderResult(subPipelineBuilder, this);
        if (!sqlBuilderResult.isEmpty()) {
            mergeSQLBuilderResult(sqlBuilderResult, false);
            isMergeGroup = true;
        }

        sqlBuilderResult = buildWindow(windowBuilder, false);
        if (sqlBuilderResult != null && !sqlBuilderResult.isEmpty()) {
            mergeSQLBuilderResult(sqlBuilderResult, isMergeGroup);
            isMergeGroup = true;
        }

        sqlBuilderResult = buildWindow(overWindowBuilder, true);
        if (sqlBuilderResult != null && !sqlBuilderResult.isEmpty()) {
            mergeSQLBuilderResult(sqlBuilderResult, isMergeGroup);
            isMergeGroup = true;
        }

        subPipelineBuilder = createPipelineBuilder();
        buildScript(subPipelineBuilder, getSelectScripts(), true);
        buildSelect(subPipelineBuilder);
        sqlBuilderResult = new SqlBuilderResult(subPipelineBuilder, this);
        if (sqlBuilderResult != null && !sqlBuilderResult.isEmpty()) {
            mergeSQLBuilderResult(sqlBuilderResult, isMergeGroup);
            isMergeGroup = true;
        }

        AbstractChainStage<?> first = CollectionUtil.isNotEmpty(pipelineBuilder.getFirstStages()) ? pipelineBuilder.getFirstStages().get(0) : null;
        SqlBuilderResult result = new SqlBuilderResult(pipelineBuilder, first, pipelineBuilder.getCurrentChainStage());
        result.getStageGroup().setSql(this.createSQLFromParser());
        result.getStageGroup().setViewName(getFromSql());
        return result;
    }

    /**
     * 把解析字段过程中生成的脚本，生成pipline的stage
     */
    protected void buildScript(PipelineBuilder pipelineBuilder, List<String> scripts, boolean isSelect) {
        String script = createScript(scripts);
        if (script != null) {
            AbstractChainStage chainStage = pipelineBuilder.addChainStage(new ScriptOperator(script));
            if (isSelect) {
                chainStage.setSql(selectSql);
                chainStage.setDiscription("SELECT Function");
            } else {
                StringBuilder stringBuilder = new StringBuilder();
                for (String sql : this.getExpressionFunctionSql()) {
                    stringBuilder.append(sql + ";" + PrintUtil.LINE);
                }
                chainStage.setSql(stringBuilder.toString());
                chainStage.setDiscription("Where Function");
            }

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
    protected void bulidExpression(PipelineBuilder pipelineBuilder) {
        if (StringUtil.isNotEmpty(expression)) {
            SqlSelect sqlSelect = (SqlSelect)sqlNode;
            sqlSelect.setWhere(null);
            String ruleName = NameCreatorContext.get().createOrGet(this.getPipelineBuilder().getPipelineName()).createName(this.getPipelineBuilder().getPipelineName(), "rule");
            AbstractChainStage chainStage = pipelineBuilder.addChainStage(new FilterOperator(getNamespace(), ruleName, expression));
            chainStage.setSql(whereSql);
            chainStage.setDiscription("Where");
        }

    }

    /**
     * build group，创建window对象
     */
    protected SqlBuilderResult buildWindow(WindowBuilder windowBuilder, boolean isOver) {
        if (windowBuilder != null) {
            windowBuilder.setConfiguration(getConfiguration());
            PipelineBuilder subPipelineBuilder = createPipelineBuilder();
            windowBuilder.setPipelineBuilder(subPipelineBuilder);
            windowBuilder.setOwner(this);
            if (CollectionUtil.isNotEmpty(hits)) {
                Map<String, String> hitsValue = hits.get(WINDOW_HITS_NAME);
                if (hitsValue != null) {
                    setWindowInfoByHits(windowBuilder, hitsValue);
                }
            }

            SqlBuilderResult sqlBuilderResult = windowBuilder.buildSql();
            if (isOver) {
                String orderby = "proctime()";
                if (CollectionUtil.isNotEmpty(windowBuilder.getShuffleOverWindowOrderByFieldNames())) {
                    orderby = MapKeyUtil.createKey(",", windowBuilder.getShuffleOverWindowOrderByFieldNames());
                }
                sqlBuilderResult.getFirstStage().setSql("ROW_NUMBER() OVER (PARTITION BY " + MapKeyUtil.createKey(",", windowBuilder.getGroupByFieldNames()) + " ORDER BY " + orderby + ") AS " + windowBuilder.getOverName());
            } else {
                sqlBuilderResult.getFirstStage().setSql("GroupBy " + MapKeyUtil.createKey(",", windowBuilder.getGroupByFieldNames()));
            }

            return sqlBuilderResult;
        }
        return null;
    }

    protected void setWindowInfoByHits(WindowBuilder windowBuilder, Map<String, String> hitsValue) {

        if (hitsValue.containsKey(HITS_WINDOW_SIZE)) {
            String value = hitsValue.get(HITS_WINDOW_SIZE);
            if (FunctionUtils.isLong(value)) {
                Integer windowSize = Integer.valueOf(value);
                windowBuilder.setSlide(windowSize);
                windowBuilder.setSize(windowSize);
            }

        }

        if (hitsValue.containsKey(HITS_WINDOW_BEFORE_EMIT)) {
            String value = hitsValue.get(HITS_WINDOW_BEFORE_EMIT);
            if (FunctionUtils.isLong(value)) {
                Long emitBefore = Long.valueOf(value);
                windowBuilder.setEmitBefore(emitBefore);
            }

        }

        if (hitsValue.containsKey(HITS_WINDOW_AFTER_EMIT)) {
            String value = hitsValue.get(HITS_WINDOW_AFTER_EMIT);
            if (FunctionUtils.isLong(value)) {
                Long emitAfter = Long.valueOf(value);
                windowBuilder.setEmitAfter(emitAfter);
            }

        }

        if (hitsValue.containsKey(HITS_WINDOW_WATERMARK)) {
            String value = hitsValue.get(HITS_WINDOW_WATERMARK);
            if (FunctionUtils.isLong(value)) {
                Integer watermark = Integer.valueOf(value);
                windowBuilder.setWaterMark(watermark);
            }

        }

        if (hitsValue.containsKey(HITS_WINDOW_TIMEFIELD)) {
            String value = hitsValue.get(HITS_WINDOW_TIMEFIELD);
            windowBuilder.setTimeFieldName(value);

        }
    }

    /**
     * 把select部分的字段做处理，如果解析时生成了脚本，且未在scripts中，生成脚本 同时根据select 字段列表，去除掉无用的字段
     */
    protected void buildSelect(PipelineBuilder pipelineBuilder) {
        if (isViewSink && fieldName2ParseResult.size() == 1 && fieldName2ParseResult.keySet().iterator().next().equals("*")) {
            AbstractChainStage chainStage = pipelineBuilder.addChainStage(new ScriptOperator("empyt()"));
            chainStage.setSql(selectSql);
            chainStage.setDiscription("SELECT");
            return;
        }

        StringBuilder stringBuilder = new StringBuilder();
        Set<String> allFieldNames = new HashSet<>();
        /**
         * 把字段生成 retain函数，因为字段解析中多数脚本已经放到了scripts成员变量中，如果个别没有，也需要生成脚本
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
                    ScriptParseResult scriptParseResult = (ScriptParseResult)parseResult;
                    List<String> scripts = scriptParseResult.getScriptValueList();
                    if (scripts != null) {
                        for (String script : scripts) {
                            /**
                             * 去重，在scripts中的 已经做过处理了
                             */
                            stringBuilder.append(script + PrintUtil.LINE);
                            if (!script.endsWith(";")) {
                                stringBuilder.append(";");
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
                String distinctScript = retainScript.replace("retainField", "distinct");
                scriptValue += distinctScript;
            }
            AbstractChainStage chainStage = pipelineBuilder.addChainStage(new ScriptOperator(scriptValue));
            chainStage.setSql(selectSql);
            chainStage.setDiscription("SELECT");
            //optimizer.put(scriptValue,true);
        }

    }

    protected void buildHits() {
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
                fieldNames = FieldsOfTableForSqlTreeContext.getInstance().get().get(getTableName());
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
            if (entry.getValue() instanceof ScriptParseResult) {
                ScriptParseResult scriptParseResult = (ScriptParseResult)entry.getValue();
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

        SelectSqlBuilder current = this.subSelect;
        while (current != null) {
            dependentTables.addAll(current.parseDependentTables());
            current = current.subSelect;
        }
        SelectSqlBuilder join = this.joinSQLBuilder;
        if (join != null) {
            dependentTables.addAll(join.parseDependentTables());
        }
        UnionSqlBuilder union = this.unionSQLBuilder;
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

    public JoinSqlBuilder getJoinSQLDescriptor() {
        return joinSQLBuilder;
    }

    public void setJoinSQLDescriptor(JoinSqlBuilder joinSQLDescriptor) {
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
         * 特殊处理window_start,window_end
         */

        if (this.getWindowBuilder() != null) {
            if (WindowBuilder.WINDOW_END.equals(fieldName) || WindowBuilder.WINDOW_START.equals(fieldName) || WindowBuilder.WINDOW_TIME.equals(fieldName)) {
                return fieldName;
            }
        }

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
        Set<String> fieldNames = FieldsOfTableForSqlTreeContext.getInstance().get().get(getTableName());
        if (fieldNames != null) {
            if (fieldNames.contains(fieldName)) {
                return fieldName;
            }
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
     * 获取字段名，对于主流表，会去掉表名前缀，对于维度表，会加上表别名前缀。这里默认是主表逻辑，维度表会覆盖这个方法
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
     * 从查询的select部分查找字段
     *
     * @param subSelect
     * @param fieldName
     * @return
     */
    public String findFieldBySelect(SelectSqlBuilder subSelect, String fieldName) {
        String asteriskName = subSelect.doAllFieldName(fieldName);
        if (asteriskName != null) {
            return asteriskName;
        }
        Set<String> keySet = null;
        if (subSelect != null) {
            keySet = subSelect.getAllFieldNames();
            if (keySet.contains(fieldName)) {
                return fieldName;
            }
        }
        int index = fieldName.indexOf(".");
        if (index != -1) {
            fieldName = fieldName.substring(index + 1);
        }

        if (subSelect != null) {
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
    public SelectSqlBuilder getParentSelect() {
        return parentSelect;
    }

    public void setParentSelect(SelectSqlBuilder parentSelect) {
        this.parentSelect = parentSelect;
    }

    public SelectSqlBuilder getSubSelect() {
        return subSelect;
    }

    public void setSubSelect(SelectSqlBuilder subSelect) {
        this.subSelect = subSelect;
    }

    public Map<String, IParseResult> getFieldName2ParseResult() {
        return fieldName2ParseResult;
    }

    public WindowBuilder getWindowBuilder() {
        return windowBuilder;
    }

    public void setWindowBuilder(WindowBuilder windowBuilder) {
        //主要用于insert emit 获取最后一个window builder
        if(this.windowBuilder!=null){
            return;
        }
        SQLParseContext.setWindowBuilder(windowBuilder);
        this.windowBuilder = windowBuilder;
    }

    public List<String> getSelectScripts() {
        return selectScripts;
    }

    public void setSelectScripts(List<String> selectScripts) {
        this.selectScripts = selectScripts;
    }

    public UnionSqlBuilder getUnionSQLBuilder() {
        return unionSQLBuilder;
    }

    public void setUnionSQLBuilder(UnionSqlBuilder unionSQLBuilder) {
        this.unionSQLBuilder = unionSQLBuilder;
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

    @Override
    public String getFromSql() {
        if (this.joinSQLBuilder != null) {
            return "FROM " + this.joinSQLBuilder.createSQLFromParser();
        }
        return super.getFromSql();
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

    public void setParseStage(int parseStage) {
        this.parseStage = parseStage;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public boolean isCloseFieldCheck() {
        return closeFieldCheck;
    }

    public void setCloseFieldCheck(boolean closeFieldCheck) {
        this.closeFieldCheck = closeFieldCheck;
    }

    public List<String> getFieldNamesOrderByDeclare() {
        return fieldNamesOrderByDeclare;
    }

    public Map<String, Map<String, String>> getHits() {
        return hits;
    }

    public WindowBuilder getOverWindowBuilder() {
        return overWindowBuilder;
    }

    public void setOverWindowBuilder(WindowBuilder overWindowBuilder) {
        this.overWindowBuilder = overWindowBuilder;
    }

    public void setViewSink(boolean isViewSink) {
        this.isViewSink = true;
    }
}