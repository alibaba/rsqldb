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
package org.apache.rsqldb.runner;

import com.alibaba.rsqldb.clients.sql.SQLStream;
import com.alibaba.rsqldb.parser.entity.SqlTask;

import org.apache.rocketmq.streams.common.utils.FileUtil;

/**
 * @author junjie.cheng
 * @date 2021/11/30
 */
public class SqlAction {

    public static void main(String[] args) {
        if (args.length < 1) {
            return;
        }
        String sqlFilePath = args[0];
        String sqlFileName = sqlFilePath.substring(0, sqlFilePath.lastIndexOf("."));

        String namespace = sqlFileName;
        if (args.length > 1) {
            namespace = args[1];
        }
        String jobName = sqlFileName;
        if (args.length > 2) {
            jobName = args[2];
        }

        String sql = FileUtil.loadFileContentContainLineSign(sqlFilePath);
        SQLStream sqlStream = new SQLStream(namespace, jobName, sql);
        sqlStream.start();
    }

}
