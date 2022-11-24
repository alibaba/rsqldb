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
package com.alibaba.rsqldb.parser;

import org.junit.Test;

public class TestView {
    private static final String sql = "create table odeum(`id` INT,`name` VARCHAR, `gmt_modified` TIMESTAMP) WITH (type = 'rocketmq', topic = 'rsqldb-odeum');";

    @Test
    public void test1() throws Throwable {
        DefaultParser parser = new DefaultParser();
        parser.parse(sql);
    }

}
