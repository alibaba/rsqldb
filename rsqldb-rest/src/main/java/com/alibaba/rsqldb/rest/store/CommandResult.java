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

import java.util.concurrent.CompletableFuture;

public class CommandResult {
    private String jobId;
    private CommandStatus status;
    private Node node;

    private CompletableFuture<Boolean> putCommandFuture;
    private Object attachment;


    public CommandResult(String jobId, CommandStatus status, Node node, CompletableFuture<Boolean> putCommandFuture) {
        this.jobId = jobId;
        this.status = status;
        this.node = node;
        this.putCommandFuture = putCommandFuture;
    }

    public CommandResult(String jobId, CommandStatus status, Node node) {
        this.jobId = jobId;
        this.status = status;
        this.node = node;

    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public CommandStatus getStatus() {
        return status;
    }

    public void setStatus(CommandStatus status) {
        this.status = status;
    }

    public Node getNode() {
        return node;
    }

    public void setNode(Node node) {
        this.node = node;
    }

    public void complete() {
        this.putCommandFuture.complete(true);
    }

    public CompletableFuture<Boolean> getPutCommandFuture() {
        return putCommandFuture;
    }


    public Object getAttachment() {
        return attachment;
    }

    public void setAttachment(Object attachment) {
        this.attachment = attachment;
    }

    @Override
    public String toString() {
        return "Node=["
                + node.getContent()
                + "]";
    }
}
