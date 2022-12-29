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
package com.alibaba.rsqldb.rest.controller;

import com.alibaba.rsqldb.rest.service.RsqlService;
import com.alibaba.rsqldb.rest.store.CommandResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;


@RestController
@RequestMapping("/command")
public class RsqlController {
    private RsqlService rsqlService;

    public RsqlController(RsqlService rsqlService) {
        this.rsqlService = rsqlService;
    }

    @PostMapping("/task/submit")
    public List<String> executeSql(@RequestBody String sql, @RequestParam(value = "jobId") String jobId) {
        return this.rsqlService.executeSql(sql, jobId);
    }

    //查询任务，以及运行状态
    @PostMapping("/task/queryAll")
    public void queryTask() {
        this.rsqlService.queryTask();
    }

    @PostMapping("/task/queryById")
    public CommandResult queryTaskByJobId(String jobId) {
        return this.rsqlService.queryTaskByJobId(jobId);
    }

    //停止任务
    @PostMapping("/task/terminate")
    public Boolean terminate(String jobId) {
        this.rsqlService.terminate(jobId);
        return true;
    }


}
