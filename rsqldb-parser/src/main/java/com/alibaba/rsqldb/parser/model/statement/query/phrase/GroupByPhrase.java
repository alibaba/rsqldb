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
package com.alibaba.rsqldb.parser.model.statement.query.phrase;

import com.alibaba.rsqldb.parser.exception.SyntaxErrorException;
import com.alibaba.rsqldb.parser.model.Field;
import com.alibaba.rsqldb.parser.model.Node;
import com.alibaba.rsqldb.parser.model.statement.query.WindowInfo;

import java.util.List;

public class GroupByPhrase extends Node {
    private List<Field> groupByFields;
    private WindowInfo windowInfo;

    public GroupByPhrase(List<Field> groupByFields, WindowInfo windowInfo) {
        if (groupByFields == null || groupByFields.size() == 0) {
            throw new SyntaxErrorException("groupBy field is null.");
        }
        this.groupByFields = groupByFields;
        this.windowInfo = windowInfo;
    }

    public List<Field> getGroupByFields() {
        return groupByFields;
    }

    public void setGroupByFields(List<Field> groupByFields) {
        this.groupByFields = groupByFields;
    }

    public WindowInfo getWindowInfo() {
        return windowInfo;
    }

    public void setWindowInfo(WindowInfo windowInfo) {
        this.windowInfo = windowInfo;
    }
}
