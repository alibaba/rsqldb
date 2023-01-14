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
package com.alibaba.rsqldb.rest.controller;

import com.alibaba.rsqldb.common.exception.RSQLServerException;
import com.alibaba.rsqldb.rest.response.BaseResult;
import com.alibaba.rsqldb.rest.response.FailedResult;
import com.alibaba.rsqldb.rest.response.QueryResult;
import com.alibaba.rsqldb.rest.response.RequestStatus;
import com.alibaba.rsqldb.rest.response.SuccessResult;
import com.alibaba.rsqldb.rest.service.RsqlService;
import com.alibaba.rsqldb.rest.util.RestUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;


@RestController
@RequestMapping("/rslqdb")
public class RsqlController {
    private static final Logger logger = LoggerFactory.getLogger(RsqlController.class);

    private RsqlService rsqlService;

    public RsqlController(RsqlService rsqlService) {
        this.rsqlService = rsqlService;
    }

    @PostMapping("/submit")
    @ResponseBody
    public BaseResult executeSql(@RequestBody String sql, @RequestParam(value = "jobId") String jobId) {
        try {
            List<String> result = this.rsqlService.executeSql(sql, jobId);

            return new SuccessResult<>(result, RequestStatus.SUCCESS);
        } catch (Throwable t) {
            logger.error("executeSql error, sql=[{}], jobId=[{}]", sql, jobId, t);
            if (t instanceof RSQLServerException) {
                return new FailedResult(RestUtil.getStackInfo(t), RequestStatus.RSQLDB_SERVER_EXCEPTION);
            } else {
                return new FailedResult(RestUtil.getStackInfo(t), RequestStatus.CLIENT_EXCEPTION);
            }
        }

    }

    //查询任务，以及运行状态
    @PostMapping("/queryAll")
    public BaseResult queryTask() {
        try {
            List<QueryResult> queryTask = this.rsqlService.queryTask();

            return new SuccessResult<>(queryTask, RequestStatus.SUCCESS);
        } catch (Throwable t) {
            logger.error("queryTask error", t);

            if (t instanceof RSQLServerException) {
                return new FailedResult(RestUtil.getStackInfo(t), RequestStatus.RSQLDB_SERVER_EXCEPTION);
            } else {
                return new FailedResult(RestUtil.getStackInfo(t), RequestStatus.CLIENT_EXCEPTION);
            }
        }
    }

    @PostMapping("/queryById")
    public BaseResult queryTaskByJobId(@RequestParam(value = "jobId") String jobId) {
        try {
            QueryResult queryTask = this.rsqlService.queryTaskByJobId(jobId);

            return new SuccessResult<>(queryTask, RequestStatus.SUCCESS);
        } catch (Throwable t) {
            logger.error("queryTaskByJobId error, jobId=[{}]",jobId, t);

            if (t instanceof RSQLServerException) {
                return new FailedResult(RestUtil.getStackInfo(t), RequestStatus.RSQLDB_SERVER_EXCEPTION);
            } else {
                return new FailedResult(RestUtil.getStackInfo(t), RequestStatus.CLIENT_EXCEPTION);
            }
        }
    }

    //停止任务
    @PostMapping("/terminate")
    public BaseResult terminate(@RequestParam(value = "jobId") String jobId) {
        try {
            this.rsqlService.terminate(jobId);

            return new SuccessResult<>(RequestStatus.SUCCESS);
        } catch (Throwable t) {
            logger.error("terminate error, jobId=[{}]",jobId, t);

            if (t instanceof RSQLServerException) {
                return new FailedResult(RestUtil.getStackInfo(t), RequestStatus.RSQLDB_SERVER_EXCEPTION);
            } else {
                return new FailedResult(RestUtil.getStackInfo(t), RequestStatus.CLIENT_EXCEPTION);
            }
        }
    }

    @PostMapping("/restart")
    public BaseResult restart(@RequestParam(value = "jobId") String jobId) {
        try {
            this.rsqlService.restart(jobId);

            return new SuccessResult<>(RequestStatus.SUCCESS);
        } catch (Throwable t) {
            logger.error("restart error, jobId=[{}]",jobId, t);

            if (t instanceof RSQLServerException) {
                return new FailedResult(RestUtil.getStackInfo(t), RequestStatus.RSQLDB_SERVER_EXCEPTION);
            } else {
                return new FailedResult(RestUtil.getStackInfo(t), RequestStatus.CLIENT_EXCEPTION);
            }
        }
    }


    @PostMapping("/remove")
    public BaseResult remove(@RequestParam(value = "jobId") String jobId) {
        try {
            this.rsqlService.remove(jobId);

            return new SuccessResult<>(RequestStatus.SUCCESS);
        } catch (Throwable t) {
            logger.error("remove error, jobId=[{}]",jobId, t);

            if (t instanceof RSQLServerException) {
                return new FailedResult(RestUtil.getStackInfo(t), RequestStatus.RSQLDB_SERVER_EXCEPTION);
            } else {
                return new FailedResult(RestUtil.getStackInfo(t), RequestStatus.CLIENT_EXCEPTION);
            }
        }
    }
}
