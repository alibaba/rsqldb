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

public class SelectWindowResult extends SelectFieldResult {
    private String windowStartFieldName;
    private String windowEndFieldName;

    //用于校验select字段的信息是否与groupBy信息一致
    private WindowInfo windowInfo;

    public SelectWindowResult(Field timestampField, String windowStartFieldName, String windowEndFieldName) {
        super(timestampField);
        this.windowStartFieldName = windowStartFieldName;
        this.windowEndFieldName = windowEndFieldName;
    }

    public String getWindowStartFieldName() {
        return windowStartFieldName;
    }

    public void setWindowStartFieldName(String windowStartFieldName) {
        this.windowStartFieldName = windowStartFieldName;
    }

    public String getWindowEndFieldName() {
        return windowEndFieldName;
    }

    public void setWindowEndFieldName(String windowEndFieldName) {
        this.windowEndFieldName = windowEndFieldName;
    }

    public WindowInfo getWindowInfo() {
        return windowInfo;
    }

    public void setWindowInfo(WindowInfo windowInfo) {
        this.windowInfo = windowInfo;
    }
}
