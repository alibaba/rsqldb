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
import com.alibaba.rsqldb.parser.model.Node;
import com.alibaba.rsqldb.rest.response.BaseResult;
import com.alibaba.rsqldb.rest.response.FailedResult;
import com.alibaba.rsqldb.rest.response.QueryResult;
import com.alibaba.rsqldb.rest.response.RequestStatus;
import com.alibaba.rsqldb.rest.response.SuccessResult;
import com.alibaba.rsqldb.rest.service.RsqlService;
import com.alibaba.rsqldb.rest.util.RestUtil;
import com.alibaba.rsqldb.storage.api.Command;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;


@RestController
@RequestMapping("/rsqldb")
public class RsqlController {
    private static final Logger logger = LoggerFactory.getLogger(RsqlController.class);

    private RsqlService rsqlService;

    public RsqlController(RsqlService rsqlService) {
        this.rsqlService = rsqlService;
    }

    @PostMapping("/submit")
    @ResponseBody
    public BaseResult executeSql(@RequestBody String sql, @RequestParam(value = "jobId") String jobId,
                                 @RequestParam(value = "startJob") Boolean startJob) {
        try {
            if (startJob == null) {
                startJob = false;
            }

            List<String> result = this.rsqlService.executeSql(sql, jobId, startJob);

            return new SuccessResult<>(result, RequestStatus.SUCCESS);
        } catch (Throwable t) {
            logger.error("executeSql error, jobId=[{}], sql=[{}], error msg:{}", jobId, sql, t.getMessage(), t);
            if (t instanceof RSQLServerException) {
                return new FailedResult(t.getMessage(), RequestStatus.RSQLDB_SERVER_EXCEPTION);
            } else {
                return new FailedResult(t.getMessage(), RequestStatus.CLIENT_EXCEPTION);
            }
        }

    }

    //查询任务，以及运行状态
    @PostMapping("/queryAll")
    public BaseResult queryTask() {
        try {
            List<Command> queryTask = this.rsqlService.queryTask();

            ArrayList<QueryResult> list = new ArrayList<>();
            for (Command command : queryTask) {
                Node node = command.getNode();
                QueryResult result;
                if (node != null) {
                    result = new QueryResult(command.getJobId(), node.getContent(), command.getStatus());
                } else {
                    result = new QueryResult(command.getJobId(), null, command.getStatus());
                }
                list.add(result);
            }

            return new SuccessResult<>(list, RequestStatus.SUCCESS);
        } catch (Throwable t) {
            logger.error("remove error, error message:{}", t.getMessage(), t);

            if (t instanceof RSQLServerException) {
                return new FailedResult(t.getMessage(), RequestStatus.RSQLDB_SERVER_EXCEPTION);
            } else {
                return new FailedResult(t.getMessage(), RequestStatus.CLIENT_EXCEPTION);
            }
        }
    }

    @PostMapping("/queryById")
    public BaseResult queryTaskByJobId(@RequestParam(value = "jobId") String jobId) {
        if (StringUtils.isBlank(jobId)) {
            return buildReturn();
        }

        try {
            Command command = this.rsqlService.queryTaskByJobId(jobId);
            Node node = command.getNode();

            QueryResult result;
            if (node != null) {
                result = new QueryResult(command.getJobId(), node.getContent(), command.getStatus());
            } else {
                result = new QueryResult(command.getJobId(), null, command.getStatus());
            }

            return new SuccessResult<>(result, RequestStatus.SUCCESS);
        } catch (Throwable t) {
            logger.error("remove error, jobId=[{}], error message:{}", jobId, t.getMessage(), t);

            if (t instanceof RSQLServerException) {
                return new FailedResult(t.getMessage(), RequestStatus.RSQLDB_SERVER_EXCEPTION);
            } else {
                return new FailedResult(t.getMessage(), RequestStatus.CLIENT_EXCEPTION);
            }
        }
    }

    //停止任务
    @PostMapping("/terminate")
    public BaseResult terminate(@RequestParam(value = "jobId") String jobId) {
        if (StringUtils.isBlank(jobId)) {
            return buildReturn();
        }

        try {
            this.rsqlService.terminate(jobId);

            return new SuccessResult<>(jobId, RequestStatus.SUCCESS);
        } catch (Throwable t) {
            logger.error("remove error, jobId=[{}], error message:{}", jobId, t.getMessage(), t);

            if (t instanceof RSQLServerException) {
                return new FailedResult(t.getMessage(), RequestStatus.RSQLDB_SERVER_EXCEPTION);
            } else {
                return new FailedResult(t.getMessage(), RequestStatus.CLIENT_EXCEPTION);
            }
        }
    }

    @PostMapping("/restart")
    public BaseResult restart(@RequestParam(value = "jobId") String jobId) {
        if (StringUtils.isBlank(jobId)) {
            return buildReturn();
        }

        try {
            this.rsqlService.restart(jobId);

            return new SuccessResult<>(jobId, RequestStatus.SUCCESS);
        } catch (Throwable t) {
            logger.error("remove error, jobId=[{}], error message:{}", jobId, t.getMessage(), t);

            if (t instanceof RSQLServerException) {
                return new FailedResult(t.getMessage(), RequestStatus.RSQLDB_SERVER_EXCEPTION);
            } else {
                return new FailedResult(t.getMessage(), RequestStatus.CLIENT_EXCEPTION);
            }
        }
    }


    @PostMapping("/remove")
    public BaseResult remove(@RequestParam(value = "jobId") String jobId) {
        if (StringUtils.isBlank(jobId)) {
            return buildReturn();
        }

        try {
            this.rsqlService.remove(jobId);

            return new SuccessResult<>(jobId, RequestStatus.SUCCESS);
        } catch (Throwable t) {
            logger.error("remove error, jobId=[{}], error message:{}", jobId, t.getMessage(), t);

            if (t instanceof RSQLServerException) {
                return new FailedResult(t.getMessage(), RequestStatus.RSQLDB_SERVER_EXCEPTION);
            } else {
                return new FailedResult(t.getMessage(), RequestStatus.CLIENT_EXCEPTION);
            }
        }
    }

    private BaseResult buildReturn() {
        return new FailedResult("jobId is indispensable.", RequestStatus.CLIENT_EXCEPTION);
    }
}
