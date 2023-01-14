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

import com.alibaba.rsqldb.parser.model.baseType.Literal;
import org.antlr.v4.runtime.ParserRuleContext;
import org.apache.rocketmq.streams.core.util.Pair;

import java.util.ArrayList;
import java.util.List;

public class TableProperties extends Node {
    private List<Pair<String, Literal<?>>> holder = new ArrayList<>();

    public TableProperties(String content) {
        super(content);
    }

    public void addProperties(String key, Literal<?> value) {
        Pair<String, Literal<?>> pair = new Pair<>(key, value);
        holder.add(pair);
    }

    public void addProperties(List<Pair<String, Literal<?>>> holder) {
        this.holder.addAll(holder);
    }

    public List<Pair<String, Literal<?>>> getHolder() {
        return holder;
    }

    public void setHolder(List<Pair<String, Literal<?>>> holder) {
        this.holder = holder;
    }
}
