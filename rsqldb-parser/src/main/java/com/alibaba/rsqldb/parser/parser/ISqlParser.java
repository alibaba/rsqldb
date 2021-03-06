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
package com.alibaba.rsqldb.parser.parser;

import com.alibaba.rsqldb.parser.parser.builder.AbstractSQLBuilder;
import com.alibaba.rsqldb.parser.parser.result.IParseResult;

public interface ISqlParser<T, DESCRIPTOR extends AbstractSQLBuilder> {

    /**
     * 解析sql节点，并把解析的内容放到builder,主要把select部分解析成script，把where部分解析成 filter
     *
     * @param
     * @param t
     */
    IParseResult parse(DESCRIPTOR builder, T t);

    /**
     * 是否支持这种node的解析
     *
     * @param sqlNode
     * @return
     */
    boolean support(Object sqlNode);

}
