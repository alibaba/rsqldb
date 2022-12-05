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

public class TestInsertValueStatement {

    @Test
    public void insert1() throws Throwable {
        String sql = "INSERT INTO `purchaser_dim` VALUES (1,'tom','male','16');";

        DefaultParser parser = new DefaultParser();
        parser.parse(sql);
    }

    @Test
    public void insert2() throws Throwable {
        String sql = "INSERT INTO Customers (CustomerName, ContactName, Address, City, PostalCode, Country)\n" +
                "VALUES ('Cardinal', 'Tom B. Erichsen', 'Skagen 21', 'Stavanger', '4006', 'Norway');";

        DefaultParser parser = new DefaultParser();
        parser.parse(sql);
    }

    @Test
    public void insert3() throws Throwable {
        String sql = "insert into test_sink\n" +
                "select field_1\n" +
                "     , field_2\n" +
                "     , field_3\n" +
                "     , field_4\n" +
                "from test_source where field_1='1';";

        DefaultParser parser = new DefaultParser();
        parser.parse(sql);
    }
}