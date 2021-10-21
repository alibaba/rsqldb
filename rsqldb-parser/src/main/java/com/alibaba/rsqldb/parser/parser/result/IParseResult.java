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
package com.alibaba.rsqldb.parser.parser.result;

public interface IParseResult<T> {

    /**
     * 如果获取的值是为了做子表达式子，会去掉最后的分号
     *
     * @return
     */
    String getValueForSubExpression();

    /**
     * 获取本次结果期望的返回值
     *
     * @return
     */
    String getReturnValue();

    ///**
    // * 如果有as 则这里是as对应的别名
    // * @return
    // */
    //String getAsName();

    /**
     * 获取最终结果，如脚本获取全部脚本。如果是常量，返回对应type的值
     *
     * @return
     */
    T getResultValue();

    /**
     * 是否是字符串，只有常量有效
     *
     * @return
     */
    boolean isConstantString();

}
