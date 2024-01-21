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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.alibaba.rsqldb.parser.SqlBuilderResult;
import com.alibaba.rsqldb.parser.sql.context.RootCreateSqlBuilderForSqlTreeContext;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.rocketmq.streams.common.configuration.ConfigurationKey;
import org.apache.rocketmq.streams.common.model.NameCreatorContext;
import org.apache.rocketmq.streams.common.topology.IStageBuilder;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.common.topology.model.AbstractChainStage;
import org.apache.rocketmq.streams.common.topology.stages.EmptyChainStage;
import org.apache.rocketmq.streams.common.topology.stages.ShuffleChainStage;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.filter.builder.ExpressionBuilder;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;
import org.apache.rocketmq.streams.filter.optimization.dependency.ScriptDependent;
import org.apache.rocketmq.streams.script.service.IScriptExpression;
import org.apache.rocketmq.streams.window.WindowConstants;
import org.apache.rocketmq.streams.window.operator.AbstractWindow;
import org.apache.rocketmq.streams.window.operator.impl.SessionOperator;
import org.apache.rocketmq.streams.window.operator.impl.ShuffleOverWindow;
import org.apache.rocketmq.streams.window.operator.impl.WindowOperator;

public class WindowBuilder extends SelectSqlBuilder {
    public static String WINDOW_START = AbstractWindow.WINDOW_START;
    public static String WINDOW_END = AbstractWindow.WINDOW_END;
    public static String WINDOW_TIME = AbstractWindow.WINDOW_TIME;
    /**
     * 窗口大小
     */
    protected Integer size;

    /**
     * 窗口周期
     */
    protected Integer slide;

    /**
     * 会话窗口的超时时间
     */
    protected Integer timeout;

    /**
     * the size of window can be set by this variable of message
     */
    protected String sizeVariable;

    /**
     * the slide of window can be set by this variable of message
     */
    protected String slideVariable;

    /**
     * the coefficient to become minute unit, such as hour unit and this value should be set to 60
     */
    protected int sizeAdjust;

    /**
     * the coefficient to become minute unit, such as hour unit and this value should be set to 60
     */
    protected int slideAdjust;

    /**
     * window type: tumble or hop
     */
    protected String type;

    /**
     * 时间字段
     */
    protected String timeFieldName;
    protected int timeUnitAdjust = 60;

    protected Integer waterMark;

    protected List<String> groupByFieldNames = new ArrayList<>();
    protected List<String> rollupFieldNames;
    protected List<String> groupSetFieldNames;
    protected Map<String, String> selectMap = new HashMap<>(32);

    protected SelectSqlBuilder owner;
    /**
     * 是否只用本地存储
     */
    protected boolean isLocalStorageOnly = true;
    protected String having;
    protected List<String> havingScript;
    /**
     * over window, 值为over时，是over window
     */
    protected String overWindowName;
    protected boolean isShuffleOverWindow = false;
    protected List<String> shuffleOverWindowOrderByFieldNames;
    protected int overWindowTopN = 100;

    protected Long emitBefore;
    protected Long emitAfter;

    protected boolean isOutputWindowInstanceInfo = false;//是否输出window_start,window_end time

    @Override
    public SqlBuilderResult buildSql() {
        AbstractWindow window = null;
        if (overWindowName != null) {
            window = buildOverWindow();
        }
        if (window == null) {
            window = org.apache.rocketmq.streams.window.builder.WindowBuilder.createWindow(type);
        }

        String value = getConfiguration().getProperty(ConfigurationKey.WINDOW_STORAGE_IS_LOCAL);
        if (StringUtil.isNotEmpty(value)) {
            isLocalStorageOnly = Boolean.valueOf(value);
        }
        window.setLocalStorageOnly(isLocalStorageOnly);
        window.setTimeFieldName(timeFieldName);
        window.setWindowType(type);
        window.setTimeUnitAdjust(1);
        CreateSqlBuilder createSQLBuilder = RootCreateSqlBuilderForSqlTreeContext.getInstance().get();
        if (createSQLBuilder.getWaterMark() != null) {
            window.setWaterMarkMinute(createSQLBuilder.getWaterMark().getWaterMarkSecond());
            if (StringUtil.isEmpty(timeFieldName)) {
                window.setTimeFieldName(createSQLBuilder.getWaterMark().getTimeFieldName());
            }
        }

        if (window instanceof WindowOperator) {
            window.setSizeInterval(Optional.ofNullable(size).orElse(WindowConstants.DEFAULT_WINDOW_SIZE * 60));
            window.setSizeVariable(sizeVariable);
            window.setSizeAdjust(sizeAdjust);

            window.setSlideInterval(Optional.ofNullable(slide).orElse(window.getSizeInterval()));
            window.setSlideVariable(slideVariable);
            window.setSlideAdjust(slideAdjust);
        }

        if (emitBefore != null) {
            window.setEmitBeforeValue(emitBefore);
        }
        if (emitAfter != null) {
            window.setEmitAfterValue(emitAfter);
        }
        if (waterMark != null) {
            window.setWaterMarkMinute(waterMark);
        }

        if (CollectionUtil.isNotEmpty(this.rollupFieldNames)) {
            window.setRollupGroupByFieldNames(this.rollupFieldNames);
            window.setSupportRollup(true);
        }
        window.setOutputWindowInstanceInfo(isOutputWindowInstanceInfo);
        if (window instanceof SessionOperator) {
            SessionOperator theWindow = (SessionOperator)window;
            theWindow.setSessionTimeOut(Optional.ofNullable(timeout).orElse(WindowConstants.DEFAULT_WINDOW_SESSION_TIMEOUT));
        }

        window.setSelectMap(selectMap);
        //        if (owner.getFieldName2ParseResult() != null) {
        //            //select部分处理，map：key字段名，value：脚本或字段名
        //            Iterator<Entry<String, IParseResult>> it = owner.getFieldName2ParseResult().entrySet().iterator();
        //            while (it.hasNext()) {
        //                Entry<String, IParseResult> entry = it.next();
        //
        //                if (!ScriptParseResult.class.isInstance(entry.getValue())||CollectionUtil.isEmpty(((ScriptParseResult)entry.getValue()).getScriptValueList())) {
        //                    selectMap.put(entry.getKey(), entry.getKey());
        //                } else {
        //                    ScriptParseResult scriptParseResult = (ScriptParseResult) entry.getValue();
        //                    selectMap.put(entry.getKey(), scriptParseResult.getScript());
        //                }
        //            }
        //            window.setSelectMap(selectMap);
        //        }
        buildHaving(window);
        /**
         * group by按字段顺序拼接成字符串，；号分割
         */
        window.setGroupByFieldName(MapKeyUtil.createKey(";", groupByFieldNames));
        if (!needTopology()) {
            Pair<AbstractChainStage, AbstractChainStage> pair = addWindowStage(window);
            return new SqlBuilderResult(getPipelineBuilder(), pair.getLeft(), pair.getRight());
        } else {
            AbstractChainStage startStage = getPipelineBuilder().addChainStage(new IStageBuilder<>() {
                @Override
                public AbstractChainStage createStageChain(PipelineBuilder pipelineBuilder) {
                    EmptyChainStage emptyChainStage = new EmptyChainStage();
                    emptyChainStage.setLabel(NameCreatorContext.get().createName("Window_Start"));
                    return emptyChainStage;
                }

                @Override
                public void addConfigurables(PipelineBuilder pipelineBuilder) {

                }
            });
            AbstractChainStage endStage = getPipelineBuilder().addChainStage(new IStageBuilder<>() {
                @Override
                public AbstractChainStage createStageChain(PipelineBuilder pipelineBuilder) {
                    EmptyChainStage emptyChainStage = new EmptyChainStage();
                    emptyChainStage.setLabel(NameCreatorContext.get().createName("Window_End"));
                    return emptyChainStage;
                }

                @Override
                public void addConfigurables(PipelineBuilder pipelineBuilder) {

                }
            });
            getPipelineBuilder().setHorizontalStages(startStage);
            getPipelineBuilder().setCurrentChainStage(startStage);
            if (CollectionUtil.isNotEmpty(this.rollupFieldNames)) {
                addWindowStage(window);
                getPipelineBuilder().setHorizontalStages(endStage);
            }

            List<String> groupSetFieldNameList = new ArrayList<>();
            if (CollectionUtil.isNotEmpty(this.groupSetFieldNames)) {
                groupSetFieldNameList.addAll(this.groupSetFieldNames);
            }
            if (CollectionUtil.isNotEmpty(this.rollupFieldNames)) {
                groupSetFieldNameList.add(null);
            }

            for (String groupName : groupSetFieldNameList) {
                getPipelineBuilder().setCurrentChainStage(startStage);
                AbstractWindow copy = window.copy();
                if (StringUtil.isEmpty(groupName)) {
                    copy.setRollupGroupByFieldNames(null);
                } else {
                    List<String> list = new ArrayList<>();
                    list.add(groupName);
                    copy.setRollupGroupByFieldNames(list);
                }

                copy.setName(NameCreatorContext.get().createName(window.getName(), "rollup", "globle"));
                addWindowStage(copy);
                getPipelineBuilder().setHorizontalStages(endStage);
                getPipelineBuilder().setCurrentChainStage(endStage);
            }

            return new SqlBuilderResult(getPipelineBuilder(), startStage, endStage);

        }
    }

    protected Pair<AbstractChainStage, AbstractChainStage> addWindowStage(AbstractWindow window) {
        AbstractChainStage windowStage = getPipelineBuilder().addChainStage(window);
        AbstractChainStage firstStage = windowStage;
        AbstractChainStage lastStage = windowStage;
        if (windowStage instanceof ShuffleChainStage) {
            getPipelineBuilder().getPipeline().getStages().remove(windowStage);
            ShuffleChainStage shuffleChainStage = (ShuffleChainStage)windowStage;
            firstStage = shuffleChainStage.getOutputChainStage();
            lastStage = shuffleChainStage.getConsumeChainStage();
            getPipelineBuilder().addChainStage(firstStage);
            getPipelineBuilder().addChainStage(lastStage);
            getPipelineBuilder().setHorizontalStages(firstStage);
            getPipelineBuilder().setCurrentChainStage(firstStage);
            getPipelineBuilder().setCurrentChainStage(lastStage);

        } else {
            getPipelineBuilder().setHorizontalStages(windowStage);
            getPipelineBuilder().setCurrentChainStage(windowStage);
        }
        return Pair.of(firstStage, lastStage);
    }

    protected boolean needTopology() {
        return CollectionUtil.isNotEmpty(this.rollupFieldNames) || CollectionUtil.isNotEmpty(this.groupSetFieldNames);
    }

    protected void buildHaving(AbstractWindow window) {
        if (StringUtil.isEmpty(having)) {
            return;
        }
        window.setHavingExpression(having);
        if (havingScript != null) {
            List<Expression> expressions = new ArrayList<>();
            ExpressionBuilder.createExpression("tmp", "tmp", having, expressions, new ArrayList<>());
            for (Expression expression : expressions) {
                String varName = expression.getVarName();
                List<String> dependentScripts = getDependentScripts(varName, havingScript);
                if (CollectionUtil.isNotEmpty(dependentScripts)) {
                    window.getSelectMap().put(varName, MapKeyUtil.createKey(";", dependentScripts) + ";");
                }
            }
        }

    }

    private List<String> getDependentScripts(String varName, List<String> script) {
        ScriptDependent scriptDependent = new ScriptDependent(null, MapKeyUtil.createKey(";", script) + ";");
        List<IScriptExpression> scriptExpressions = scriptDependent.getDependencyExpression(varName);
        List<String> expressions = new ArrayList<>();
        for (IScriptExpression scriptExpression : scriptExpressions) {
            expressions.add(scriptExpression.toString());
        }
        return expressions;
    }

    protected AbstractWindow buildOverWindow() {
        AbstractWindow overWindow = null;
        String groupBy = MapKeyUtil.createKey(";", groupByFieldNames);
        if (!isShuffleOverWindow) {
            overWindow = org.apache.rocketmq.streams.window.builder.WindowBuilder.createOvertWindow(groupBy, overWindowName);
        } else {
            ShuffleOverWindow shuffleOverWindow = new ShuffleOverWindow();
            shuffleOverWindow.setTimeFieldName("");
            shuffleOverWindow.setGroupByFieldName(groupBy);
            shuffleOverWindow.setRowNumerName(overWindowName);
            shuffleOverWindow.setTopN(overWindowTopN);
            shuffleOverWindow.setOrderFieldNames(shuffleOverWindowOrderByFieldNames);
            overWindow = shuffleOverWindow;
        }

        return overWindow;
    }

    @Override
    public Set<String> parseDependentTables() {
        return new HashSet<>();
    }

    public SelectSqlBuilder getOwner() {
        return owner;
    }

    public void setOwner(SelectSqlBuilder owner) {
        this.owner = owner;
    }

    public Integer getSize() {
        return size;
    }

    public void setSize(Integer size) {
        this.size = size;
    }

    public Integer getSlide() {
        return slide;
    }

    public void setSlide(Integer slide) {
        this.slide = slide;
    }

    public String getTimeFieldName() {
        return timeFieldName;
    }

    public void setTimeFieldName(String timeFieldName) {
        this.timeFieldName = timeFieldName;
    }

    public List<String> getGroupByFieldNames() {
        return groupByFieldNames;
    }

    public void setGroupByFieldNames(List<String> groupByFieldNames) {
        this.groupByFieldNames = groupByFieldNames;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getOverWindowName() {
        return overWindowName;
    }

    public void setOverWindowName(String overWindowName) {
        this.overWindowName = overWindowName;
    }

    public String getSizeVariable() {
        return sizeVariable;
    }

    public void setSizeVariable(String sizeVariable) {
        this.sizeVariable = sizeVariable;
    }

    public String getSlideVariable() {
        return slideVariable;
    }

    public void setSlideVariable(String slideVariable) {
        this.slideVariable = slideVariable;
    }

    public int getSizeAdjust() {
        return sizeAdjust;
    }

    public void setSizeAdjust(int sizeAdjust) {
        this.sizeAdjust = sizeAdjust;
    }

    public int getSlideAdjust() {
        return slideAdjust;
    }

    public void setSlideAdjust(int slideAdjust) {
        this.slideAdjust = slideAdjust;
    }

    public boolean isLocalStorageOnly() {
        return isLocalStorageOnly;
    }

    public void setLocalStorageOnly(boolean localStorageOnly) {
        isLocalStorageOnly = localStorageOnly;
    }

    public Integer getTimeout() {
        return timeout;
    }

    public void setTimeout(Integer timeout) {
        this.timeout = timeout;
    }

    public boolean isShuffleOverWindow() {
        return isShuffleOverWindow;
    }

    public void setShuffleOverWindow(boolean shuffleOverWindow) {
        isShuffleOverWindow = shuffleOverWindow;
    }

    public List<String> getShuffleOverWindowOrderByFieldNames() {
        return shuffleOverWindowOrderByFieldNames;
    }

    public void setShuffleOverWindowOrderByFieldNames(List<String> shuffleOverWindowOrderByFieldNames) {
        this.shuffleOverWindowOrderByFieldNames = shuffleOverWindowOrderByFieldNames;
    }

    public int getOverWindowTopN() {
        return overWindowTopN;
    }

    public void setOverWindowTopN(int overWindowTopN) {
        this.overWindowTopN = overWindowTopN;
    }

    public String getHaving() {
        return having;
    }

    public void setHaving(String having) {
        this.having = having;
    }

    public List<String> getHavingScript() {
        return havingScript;
    }

    public void setHavingScript(List<String> havingScript) {
        this.havingScript = havingScript;
    }

    public int getTimeUnitAdjust() {
        return timeUnitAdjust;
    }

    public void setTimeUnitAdjust(int timeUnitAdjust) {
        this.timeUnitAdjust = timeUnitAdjust;
    }

    public Long getEmitBefore() {
        return emitBefore;
    }

    public void setEmitBefore(Long emitBefore) {
        this.emitBefore = emitBefore;
    }

    public Long getEmitAfter() {
        return emitAfter;
    }

    public void setEmitAfter(Long emitAfter) {
        this.emitAfter = emitAfter;
    }

    public Integer getWaterMark() {
        return waterMark;
    }

    public void setWaterMark(Integer waterMark) {
        this.waterMark = waterMark;
    }

    public boolean isOutputWindowInstanceInfo() {
        return isOutputWindowInstanceInfo;
    }

    public void setOutputWindowInstanceInfo(boolean outputWindowInstanceInfo) {
        isOutputWindowInstanceInfo = outputWindowInstanceInfo;
    }

    public List<String> getRollupFieldNames() {
        return rollupFieldNames;
    }

    public void setRollupFieldNames(List<String> rollupFieldNames) {
        this.rollupFieldNames = rollupFieldNames;
    }

    public List<String> getGroupSetFieldNames() {
        return groupSetFieldNames;
    }

    public void setGroupSetFieldNames(List<String> groupSetFieldNames) {
        this.groupSetFieldNames = groupSetFieldNames;
    }

    public Map<String, String> getSelectMap() {
        return selectMap;
    }
}
