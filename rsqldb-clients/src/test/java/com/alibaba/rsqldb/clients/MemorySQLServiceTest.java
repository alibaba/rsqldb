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
package com.alibaba.rsqldb.clients;

import java.util.List;

import com.alibaba.rsqldb.clients.manager.SQLForJob;
import com.alibaba.rsqldb.clients.manager.SQLManager;

import org.junit.Test;

public class MemorySQLServiceTest {
    @Test
    public void testSQLService() {
        SQLForJob sqlForJob1 = new SQLForJob();
        sqlForJob1.setNameSpace("test_namespace");
        sqlForJob1.setName("test");
        sqlForJob1.setSql("sql");
        sqlForJob1.setVersion(System.currentTimeMillis());

        SQLForJob sqlForJob2 = new SQLForJob();
        sqlForJob2.setNameSpace("test_namespace");
        sqlForJob2.setName("test2");
        sqlForJob2.setSql("sql");
        sqlForJob2.setVersion(System.currentTimeMillis());

        SQLForJob sqlForJob3 = new SQLForJob();
        sqlForJob3.setNameSpace("test_namespace1");
        sqlForJob3.setName("test");
        sqlForJob3.setSql("sql");
        sqlForJob3.setVersion(System.currentTimeMillis());

        SQLManager sqlManager = SQLManager.createFromDir("namespace", "/tmp/sql");
        sqlManager.saveOrUpdate(sqlForJob1);
        sqlManager.saveOrUpdate(sqlForJob2);
        sqlManager.saveOrUpdate(sqlForJob3);

        List<SQLForJob> sqlForJobs = sqlManager.select();
        System.out.println(sqlForJobs.size() == 2);

        SQLForJob sqlForJob = sqlManager.select("test");
        System.out.println(sqlForJob != null);

        sqlManager.delete("test");

        sqlForJob = sqlManager.select("test");
        System.out.println(sqlForJob != null);
    }

}
