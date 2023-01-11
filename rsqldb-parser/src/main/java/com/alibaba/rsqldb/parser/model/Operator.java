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

//todo support like.
public enum Operator {
    EQUAL("="), GREATER(">"), LESS("<"), NOT_EQUAL("!=", "<>"),
    GREATER_EQUAL(">="), LESS_EQUAL("<="), BETWEEN_AND("between_and"), IN("in"), AND("and"), OR("or");

    private String symbol;
    private String nickName;

    Operator(String symbol) {
        this.symbol = symbol;
    }

    Operator(String symbol, String nickName) {
        this.symbol = symbol;
        this.nickName = nickName;
    }

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public String getNickName() {
        return nickName;
    }

    public void setNickName(String nickName) {
        this.nickName = nickName;
    }
}
