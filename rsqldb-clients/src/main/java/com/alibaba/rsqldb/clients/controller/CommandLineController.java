package com.alibaba.rsqldb.clients.controller;

import com.alibaba.rsqldb.clients.service.ITaskService;
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
    public String listTasks(@RequestParam(value = "namespace", defaultValue = "default") String namespace) {
        try {
            return taskService.list(namespace);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return String.format("Task for %s list success!", namespace);
    }

    @PostMapping("/task/submit")
    public String submitTask(@RequestParam(value = "namespace", defaultValue = "default") String namespace, @RequestParam(value = "taskName") String taskName, @RequestBody String sql) {
        try {
            taskService.submit(namespace, taskName, sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return String.format("Task %s|%s submit success!", namespace, taskName);
    }

    @PostMapping("/task/submit/file")
    public String submitTaskFile(@RequestParam(value = "namespace", defaultValue = "default") String namespace, @RequestParam(value = "taskName") String taskName, @RequestBody String sqlPath) {
        try {
            taskService.submitFile(namespace, taskName, sqlPath);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return String.format("Task %s|%s submit success!", namespace, taskName);
    }

    @PostMapping("/task/start")
    public String startTask(@RequestParam(value = "namespace", defaultValue = "default") String namespace, @RequestParam(value = "taskName") String taskName) {
        try {
            taskService.start(namespace, taskName);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return String.format("Task %s|%s start success", namespace, taskName);
    }

    @PostMapping("/task/stop")
    public String stopTask(@RequestParam(value = "namespace", defaultValue = "default") String namespace, @RequestParam(value = "taskName") String taskName) {
        try {
            taskService.stop(namespace, taskName);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return String.format("Task %s|%s start success", taskName, taskName);
    }
}
