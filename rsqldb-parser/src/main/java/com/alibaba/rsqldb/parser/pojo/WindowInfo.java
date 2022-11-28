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
package com.alibaba.rsqldb.parser.pojo;


import java.util.concurrent.TimeUnit;

public class WindowInfo {
    public enum WindowType {
        TUMBLE, HOP, SESSION
    }

    public enum TargetTime {
        WINDOW_START, WINDOW_END
    }

    private WindowType type;
    private long slide;
    private long size;
    private TimeUnit timeUnit;
    private Field timeField;

    private TargetTime targetTime;

    public WindowInfo(WindowType type, long slide, long size, TimeUnit timeUnit, Field timeField) {
        this.type = type;
        this.slide = slide;
        this.size = size;
        this.timeUnit = timeUnit;
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
}
