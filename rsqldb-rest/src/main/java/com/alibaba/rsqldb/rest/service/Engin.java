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
package com.alibaba.rsqldb.rest.service;

import com.alibaba.rsqldb.parser.model.statement.Statement;
import com.alibaba.rsqldb.rest.response.QueryResult;
import com.alibaba.rsqldb.rest.store.CommandStatus;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface Engin {
    void start();

    CompletableFuture<Throwable> putStatement(String jobId, Statement node) throws Throwable;

    List<QueryResult> queryAll();

    QueryResult queryByJobId(String jobId);

    void terminate(String jobId) throws Throwable;

    void restart(String jobId) throws Throwable;

    void remove(String jobId) throws Throwable;
}
