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
package com.alibaba.rsqldb.clients.sql;

import org.junit.Test;

public class TableStreamTest {
    protected String sql="CREATE FUNCTION now as 'com.sql.Function';\n"
        + "CREATE TABLE graph_vertex_proc (\n"
        + "  `time` varchar,\n"
        + "  `uuid` varchar,\n"
        + "  aliuid varchar,\n"
        + "  pid varchar,\n"
        + "  file_path varchar,\n"
        + "  cmdline varchar,\n"
        + "  tty varchar,\n"
        + "  cwd varchar,\n"
        + "  perm varchar\n"
        + ") WITH (\n"
        + " type='metaq',\n"
        + " topic='blink_dXXXXXXX',\n"
        + " pullIntervalMs='100',\n"
        + " consumerGroup='CID_BLINK_SOURCE_001',\n"
        + " fieldDelimiter='#'\n"
        + ");\n"
        + "CREATE TABLE graph_proc_label_extend (\n"
        + "  `time` varchar,\n"
        + "  `uuid` varchar,\n"
        + "  aliuid varchar,\n"
        + "  pid varchar,\n"
        + "  file_path varchar,\n"
        + "  cmdline varchar,\n"
        + "  tty varchar,\n"
        + "  cwd varchar,\n"
        + "  perm varchar\n"
        + ") WITH (type = 'print');\n"
        + "INSERT\n"
        + "  INTO graph_proc_label_extend\n"
        + "SELECT\n"
        + "  `time`,\n"
        + "  `uuid`,\n"
        + "  aliuid,\n"
        + "  pid,\n"
        + "  file_path,\n"
        + "  cmdline,\n"
        + "  tty,\n"
        + "  cwd,\n"
        + "  perm\n"
        + "FROM\n"
        + "  graph_vertex_proc;";


    /**
     * start in jar
     */
    @Test
    public void  testSQL(){
        SQLStreamClient sqlStreamClient= new SQLStreamClient("test_namespace", "test_pipeline",sql);
        sqlStreamClient.start();
    }



}
