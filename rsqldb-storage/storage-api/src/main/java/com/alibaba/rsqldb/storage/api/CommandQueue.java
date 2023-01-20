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
package com.alibaba.rsqldb.storage.api;


import com.alibaba.rsqldb.parser.model.statement.Statement;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

public interface CommandQueue extends AutoCloseable {
    void start(Properties properties);

    /**
     * 从头恢复消费，直到最后一条命令。恢复所有的命令到本地，供后续查找
     */
    CompletableFuture<Boolean> restore() throws Throwable;

    CompletableFuture<Throwable> putCommand(Command command) throws Throwable;

    CommandWrapper getNextCommand() throws Throwable;

    //可能返回CreateTableStatement，也可能返回CreateViewStatement
    Statement findTable(String tableName);

    Command queryStatus(String jobId);

    List<Command> queryStatus();

    CompletableFuture<Throwable> delete(String jobId) throws Throwable;
}
