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

import com.alibaba.rsqldb.parser.entity.SqlTask;

import org.apache.rocketmq.streams.common.optimization.fingerprint.FingerprintCache;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.common.utils.FileUtil;
import org.apache.rocketmq.streams.configurable.ConfigurableComponent;
import org.apache.rocketmq.streams.filter.optimization.dependency.DependencyTree;
import org.junit.Test;

public class PrefingerprintTest {
    private String dir = "/Users/yuanxiaodong/Documents/workdir/developer/stream-computing/dipper_sql/aegis_proc/";

    @Test
    public void testPrefingerprint() {
        String sql = FileUtil.loadFileContentContainLineSign(dir + "windows_proc_alert.sql");
        SQLStream sqlStreamClient = new SQLStream("test_namespace", "test_pipeline", sql);
        SqlTask sqlBuilder = sqlStreamClient.build(ConfigurableComponent.getInstance("test_namespace"));
        ChainPipeline pipeline = sqlBuilder.getChainPipelines().get(0);
        DependencyTree dependencyTree = new DependencyTree(pipeline, new FingerprintCache(100000));
        dependencyTree.parse();
    }

    public void testSqlStreams() {

    }

}
