 /*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rsqldb.parser.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.antlr.v4.runtime.ParserRuleContext;
import org.apache.rocketmq.streams.core.util.Pair;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Columns extends Node {
    //pair 的顺序就是create table中field顺序
    //create table odeum(`id` INT,`name` VARCHAR, `gmt_modified` TIMESTAMP) WITH (type = null, topic = 'rsqldb-odeum', data_format='JSON');
    private List<Pair<String, FieldType>> holder = new ArrayList<>();

    @JsonCreator
    public Columns(@JsonProperty("content") String content) {
        super(content);
    }

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

    public Set<String> getFields() {
        HashSet<String> fieldName = new HashSet<>();

        for (Pair<String, FieldType> pair : holder) {
            fieldName.add(pair.getKey());
        }

        return fieldName;
    }

    @Override
    public String toString() {
        return "Columns{" +
                "holder=" + holder +
                '}';
    }
}
