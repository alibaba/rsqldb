/*
 * Copyright 1999-2023 Alibaba Group Holding Ltd.
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
package com.alibaba.rsqldb.storage.rocketmq;

import com.alibaba.rsqldb.storage.api.CallBack;
import com.alibaba.rsqldb.storage.api.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

public class RocketMQCallBack implements CallBack {
    private static final Logger logger = LoggerFactory.getLogger(RocketMQCallBack.class);

    private final CompletableFuture<Throwable> completableFuture;
    private final BiConsumer<String, Command> statusCommit;
    private Runnable offsetCommit;

    public RocketMQCallBack(CompletableFuture<Throwable> completableFuture, BiConsumer<String, Command> statusCommit) {
        this.completableFuture = completableFuture;
        this.statusCommit = statusCommit;
    }

    public RocketMQCallBack(CompletableFuture<Throwable> completableFuture, BiConsumer<String, Command> statusCommit, Runnable offsetCommit) {
        this.completableFuture = completableFuture;
        this.offsetCommit = offsetCommit;
        this.statusCommit = statusCommit;
    }

    @Override
    public void onCompleted(String jobId, Command command) {
        completableFuture.complete(null);

        if (offsetCommit != null) {
            offsetCommit.run();
        }

        if (statusCommit != null) {
            statusCommit.accept(jobId, command);
        }
    }

    @Override
    public void onError(String jobId, Command command, Throwable attachment) {
        completableFuture.complete(attachment);

        if (offsetCommit != null) {
            logger.error("skip jobId:{} and commit offset.", jobId);
            offsetCommit.run();
        }

        if (statusCommit != null) {
            statusCommit.accept(jobId, command);
        }
    }
}
