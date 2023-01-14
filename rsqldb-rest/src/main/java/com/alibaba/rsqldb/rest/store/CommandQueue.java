 /*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
 import com.alibaba.rsqldb.rest.response.QueryResult;
 import com.alibaba.rsqldb.rest.service.iml.CommandOperator;
 import org.apache.rocketmq.streams.core.util.Pair;

 import java.util.List;
 import java.util.concurrent.CompletableFuture;

public interface CommandQueue {
    void start();

    /**
     * 从头恢复消费，直到最后一条命令。恢复所有的命令到本地，供后续查找
     */
    CompletableFuture<Boolean> restore() throws Throwable;

    CompletableFuture<Throwable> putStatement(String jobId, Statement statement) throws Throwable;

    CompletableFuture<Throwable> putCommand(String jobId, CommandOperator operator) throws Throwable;

    Pair<String/*jobId*/, Node> getNextCommand() throws Throwable;

    //可能返回CreateTableStatement，也可能返回CreateViewStatement
    Statement findTable(String tableName);

    QueryResult queryStatus(String jobId);

    List<QueryResult> queryStatus();

    void remove(String jobId);

    void onCompleted(String jobId, CommandStatus status);

    void onError(String jobId, CommandStatus status, Throwable attachment);

}
