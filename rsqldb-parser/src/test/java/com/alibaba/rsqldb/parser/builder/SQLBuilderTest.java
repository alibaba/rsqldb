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

import org.apache.rocketmq.streams.common.utils.FileUtil;
import org.junit.Test;

public class SQLBuilderTest {
    @Test
    public void testSQL() {
        String sqlPath = "/Users/yuanxiaodong/Desktop/dipper_sql/aegis_proc/windows_proc_alert.sql";
        String sql = FileUtil.loadFileContentContainLineSign(sqlPath);
        SQLBuilder sqlBuilder = new SQLBuilder("namespace", "name", sql);
        sqlBuilder.startSQL();
    }

    @Test
    public void testHomologousSQL() {
        String sqlPath = "/Users/yuanxiaodong/Desktop/dipper_sql/aegis_proc/windows_proc_alert.sql";
        String sql = FileUtil.loadFileContentContainLineSign(sqlPath);
        HomologousSQLBuilder sqlBuilder = new HomologousSQLBuilder("namespace", "name", sql, "aegis_proc");
        sqlBuilder.startSQL();
    }

    @Test
    public void testStreamSQL() {

        HomologousSQLBuilder sqlBuilder = new HomologousSQLBuilder("namespace", "name", "1212", "aegis_proc");
        sqlBuilder.startSQL();

    }

}
