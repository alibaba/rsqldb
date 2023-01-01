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
package com.alibaba.rsqldb.rest.store;


import com.alibaba.rsqldb.parser.model.Node;
import com.alibaba.rsqldb.parser.model.statement.Statement;
import com.alibaba.rsqldb.rest.service.iml.CommandNode;
import com.alibaba.rsqldb.rest.service.iml.CommandOperator;
import org.apache.rocketmq.streams.core.util.Pair;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public interface CommandQueue {
    void start();

    /**
     * 从头恢复消费，直到最后一条命令。恢复所有的命令到本地，供后续查找
     */
    CompletableFuture<Boolean> restore() throws Exception;

    CompletableFuture<Throwable> putStatement(String jobId, Statement statement);

    CompletableFuture<Throwable> putCommand(String jobId, CommandOperator operator);

    Pair<String/*jobId*/, Node> getNextCommand() throws Exception;

    //可能返回CreateTableStatement，也可能返回CreateViewStatement
    Statement findTable(String tableName);

    CommandStatus queryStatus(String jobId);

    Map<String, CommandStatus> queryStatus();

    void remove(String jobId);

    void onCompleted(String jobId, CommandStatus status);

    void onError(String jobId, CommandStatus status, Throwable attachment);

}
