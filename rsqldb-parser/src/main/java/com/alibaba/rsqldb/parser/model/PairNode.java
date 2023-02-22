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

import org.apache.rocketmq.streams.core.util.Pair;

public class PairNode<K, V> extends Node {
    private Pair<K, V> pair;

    public PairNode(String content, Pair<K, V> pair) {
        super(content);
        this.pair = pair;
    }


    public Pair<K, V> getPair() {
        return pair;
    }

    public void setPair(Pair<K, V> pair) {
        this.pair = pair;
    }
}
