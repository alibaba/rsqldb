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
package com.alibaba.rsqldb.parser.builder;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;

@FunctionHint(
    input = {@DataTypeHint("INT"), @DataTypeHint("INT")},
    output = @DataTypeHint("ROW<s STRING, i INT>")
)
@FunctionHint(
    input = {@DataTypeHint("BIGINT"), @DataTypeHint("BIGINT")},
    output = @DataTypeHint("ROW<s STRING, i BIGINT>")
)
@FunctionHint(
    input = {},
    output = @DataTypeHint("ROW<s STRING, i INT>")
)
public class UDTFFunction extends TableFunction {

    public void eval() {
        collect("abd");
        collect(13434);
        collect(true);
    }
}
