package com.alibaba.rsqldb.server.service;

import com.alibaba.rsqldb.server.sql.RemoteSqlStream;
import org.apache.rocketmq.streams.common.topology.task.StreamsTask;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class TaskServiceImpl implements ITaskService {

    @Override public void submit(String namespace, String taskName, String sql) throws Exception {
        RemoteSqlStream.create(namespace).init().name(taskName).sql(sql);
        System.out.println("submit task success");
    }

    @Override public void submitFile(String namespace, String taskName, String sqlPath) throws Exception {
        RemoteSqlStream.create(namespace).init().name(taskName).sqlPath(sqlPath);
    }

    @Override public void start(String namespace, String taskName) throws Exception {
        RemoteSqlStream.create(namespace).init().name(taskName).start();
        System.out.println("start task success");
    }

    @Override public void stop(String namespace, String taskName) throws Exception {
        RemoteSqlStream.create(namespace).init().name(taskName).stop();
    }

    @Override public String list(String namespace) throws Exception {
        List<StreamsTask> streamsTaskList = RemoteSqlStream.create(namespace).init().list();
        StringBuilder builder = new StringBuilder();
        builder.append("namespace").append("      ").append("task_name").append("      ").append("state").append("\n");
        builder.append("--------------------------------------------------------------------------------------------").append("\n");
        for (StreamsTask streamsTask : streamsTaskList) {
            builder.append(namespace).append("      ").append(streamsTask.getConfigureName()).append("      ").append(streamsTask.getState()).append("\n");
        }
        return builder.toString();
    }

}
