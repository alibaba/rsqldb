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
package com.alibaba.rsqldb.parser.model;

import com.alibaba.rsqldb.parser.util.Pair;

import java.util.ArrayList;
import java.util.List;

public class Columns extends Node {
    private List<Pair<String, FieldType>> holder = new ArrayList<>();

    public Columns() {}

    public void addFieldNameAndType(String fieldName, FieldType type) {
        Pair<String, FieldType> pair = new Pair<>(fieldName, type);
        holder.add(pair);
    }

    public void addColumns(List<Pair<String, FieldType>> source) {
        this.holder.addAll(source);
    }

    public List<Pair<String, FieldType>> getHolder() {
        return holder;
    }

    public void setHolder(List<Pair<String, FieldType>> holder) {
        this.holder = holder;
    }

    @Override
    public String toString() {
        return "Columns{" +
                "holder=" + holder +
                '}';
    }
}
