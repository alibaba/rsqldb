package com.alibaba.rsqldb.server.controller;

import com.alibaba.rsqldb.server.service.ITaskService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/command")
public class CommandLineController {

    private ITaskService taskService;

    @Autowired
    public void setTaskService(ITaskService taskService) {
        this.taskService = taskService;
    }

    @PostMapping("/task/list")
    public String listTasks(@RequestParam(value = "namespace", defaultValue = "default") String namespace) throws Throwable {
        try {
            return taskService.list(namespace);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }

    }

    @PostMapping("/task/submit")
    public String submitTask(@RequestParam(value = "namespace", defaultValue = "default") String namespace,
                            @RequestParam(value = "taskName") String taskName, @RequestParam String sql) throws Throwable {
        try {
            taskService.submit(namespace, taskName, sql);
            return String.format("Task %s|%s submit sql success!", namespace, taskName);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @PostMapping("/task/submit/file")
    public String submitTaskFile(@RequestParam(value = "namespace", defaultValue = "default") String namespace,
                                @RequestParam(value = "taskName") String taskName, @RequestBody String sqlPath) throws Throwable {
        try {
            taskService.submitFile(namespace, taskName, sqlPath);
            return String.format("Task %s|%s submit file success!", namespace, taskName);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @PostMapping("/task/start")
    public String startTask(@RequestParam(value = "namespace", defaultValue = "default") String namespace,
                            @RequestParam(value = "taskName") String taskName) throws Throwable {
        try {
            taskService.start(namespace, taskName);
            return String.format("Task %s|%s start success", namespace, taskName);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @PostMapping("/task/stop")
    public String stopTask(@RequestParam(value = "namespace", defaultValue = "default") String namespace,
                            @RequestParam(value = "taskName") String taskName) throws Throwable {
        try {
            taskService.stop(namespace, taskName);
            return String.format("Task %s|%s stop success", taskName, taskName);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }
}
