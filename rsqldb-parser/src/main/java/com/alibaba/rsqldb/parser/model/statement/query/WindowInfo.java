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
package com.alibaba.rsqldb.parser.model.statement.query;


import com.alibaba.rsqldb.parser.model.Field;
import com.alibaba.rsqldb.parser.model.Node;
import org.antlr.v4.runtime.ParserRuleContext;

import java.util.concurrent.TimeUnit;

public class WindowInfo extends Node {
    public enum WindowType {
        TUMBLE, HOP, SESSION
    }

    public enum TargetTime {
        WINDOW_START(-1), WINDOW_END(1);
        private int index;

        TargetTime(int index) {
            this.index = index;
        }

        public int getIndex() {
            return index;
        }
    }

    //-------------window元信息-----------------------------------------------------------------------------------------------------------
    private WindowType type;
    private long slide;
    private long size;
    private TimeUnit timeUnit = TimeUnit.SECONDS;
    private Field timeField;

    //----------------非元信息--------------------------------------------------------------------------------------------------------
    private TargetTime targetTime;
    private String newFieldName;


    public WindowInfo(ParserRuleContext context, WindowType type, long slide, long size, Field timeField) {
        super(context);
        this.type = type;
        this.slide = slide;
        this.size = size;
        this.timeField = timeField;
    }

    public WindowType getType() {
        return type;
    }

    public void setType(WindowType type) {
        this.type = type;
    }

    public long getSlide() {
        return slide;
    }

    public void setSlide(long slide) {
        this.slide = slide;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public Field getTimeField() {
        return timeField;
    }

    public void setTimeField(Field timeField) {
        this.timeField = timeField;
    }

    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

    public void setTimeUnit(TimeUnit timeUnit) {
        this.timeUnit = timeUnit;
    }

    public TargetTime getTargetTime() {
        return targetTime;
    }

    public void setTargetTime(TargetTime targetTime) {
        this.targetTime = targetTime;
    }

    public String getNewFieldName() {
        return newFieldName;
    }

    public void setNewFieldName(String newFieldName) {
        this.newFieldName = newFieldName;
    }

    @Override
    public String toString() {
        return "WindowInfo{" +
                "type=" + type +
                ", slide=" + slide +
                ", size=" + size +
                ", timeUnit=" + timeUnit +
                ", timeField=" + timeField +
                '}';
    }
}
