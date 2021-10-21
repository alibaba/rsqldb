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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import org.apache.rocketmq.streams.window.operator.AbstractWindow;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import com.alibaba.rsqldb.parser.parser.result.IParseResult;
import com.alibaba.rsqldb.parser.parser.result.ScriptParseResult;
import org.apache.rocketmq.streams.window.operator.impl.SessionOperator;
import org.apache.rocketmq.streams.window.operator.impl.ShuffleOverWindow;
import org.apache.rocketmq.streams.window.operator.impl.WindowOperator;

public class WindowBuilder extends SelectSQLBuilder {

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

    protected List<String> groupByFieldNames = new ArrayList<>();

    protected SelectSQLBuilder owner;
    protected boolean isLocalStorageOnly = true;//是否只用本地存储

    /**
     * over window
     */
    protected String overWindowName;//值为over时，是over window
    protected boolean isShuffleOverWindow=false;
    protected List<String> shuffleOverWindowOrderByFieldNames;
    protected int overWindowTopN=100;
    @Override
    protected void build() {
        AbstractWindow window;
        if (overWindowName != null) {
            if(!isShuffleOverWindow){
                buildOverWindow();
                return;
            }

            return;
        }


        window = org.apache.rocketmq.streams.window.builder.WindowBuilder.createWindow(type);
        window.setLocalStorageOnly(isLocalStorageOnly);
        window.setTimeFieldName(timeFieldName);
        window.setWindowType(type);

        if (window instanceof WindowOperator) {
            window.setSizeInterval(Optional.ofNullable(size).orElse(AbstractWindow.DEFAULT_WINDOW_SIZE));
            window.setSizeVariable(sizeVariable);
            window.setSizeAdjust(sizeAdjust);

            window.setSlideInterval(Optional.ofNullable(slide).orElse(AbstractWindow.DEFAULT_WINDOW_SLIDE));
            window.setSlideVariable(slideVariable);
            window.setSlideAdjust(slideAdjust);
        }

        if (window instanceof SessionOperator) {
            SessionOperator theWindow = (SessionOperator) window;
            theWindow.setSessionTimeOut(Optional.ofNullable(timeout).orElse(AbstractWindow.DEFAULT_WINDOW_SESSION_TIMEOUT));
        }


        Map<String, String> selectMap = new HashMap<>(32);
        if (owner.getFieldName2ParseResult() != null) {
            //select部分处理，map：key字段名，value：脚本或字段名
            Iterator<Entry<String, IParseResult>> it = owner.getFieldName2ParseResult().entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, IParseResult> entry = it.next();
                ScriptParseResult scriptParseResult = (ScriptParseResult)entry.getValue();
                if (CollectionUtil.isEmpty(scriptParseResult.getScriptValueList())) {
                    selectMap.put(entry.getKey(), entry.getKey());
                } else {
                    selectMap.put(entry.getKey(), scriptParseResult.getScript());
                }
            }
            window.setSelectMap(selectMap);
        }
        /**
         * group by按字段顺序拼接成字符串，；号分割
         */
        window.setGroupByFieldName(MapKeyUtil.createKey(";", groupByFieldNames));
        getPipelineBuilder().addChainStage(window);
    }

    protected void buildOverWindow() {
        AbstractWindow overWindow=null;
        String groupBy=MapKeyUtil.createKey(";", groupByFieldNames);
        if(!isShuffleOverWindow){
             overWindow = org.apache.rocketmq.streams.window.builder.WindowBuilder.createOvertWindow(groupBy, overWindowName);
        }else {
            ShuffleOverWindow shuffleOverWindow=new ShuffleOverWindow();
            shuffleOverWindow.setTimeFieldName("");
            shuffleOverWindow.setGroupByFieldName(groupBy);
            shuffleOverWindow.setRowNumerName(overWindowName);
            shuffleOverWindow.setTopN(overWindowTopN);
            shuffleOverWindow.setOrderFieldNames(shuffleOverWindowOrderByFieldNames);
            overWindow=shuffleOverWindow;
        }

        getPipelineBuilder().addChainStage(overWindow);
    }

    @Override
    public Set<String> parseDependentTables() {
        return new HashSet<>();
    }

    public SelectSQLBuilder getOwner() {
        return owner;
    }

    public void setOwner(SelectSQLBuilder owner) {
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
}
