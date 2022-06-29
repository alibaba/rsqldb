package com.alibaba.rsqldb.server.service;

import com.alibaba.rsqldb.server.sql.SqlStreamBuilder;
import java.util.List;
import org.apache.rocketmq.streams.common.topology.task.StreamsTask;
import org.springframework.stereotype.Service;

@Service
public class TaskServiceImpl implements ITaskService {

    @Override public void submit(String namespace, String taskName, String sql) throws Exception {
        SqlStreamBuilder.remoteSqlStream(namespace).file().fileConfig().init().name(taskName).sql(sql);
        System.out.println("submit task success");
    }

    @Override public void submitFile(String namespace, String taskName, String sqlPath) throws Exception {
        SqlStreamBuilder.remoteSqlStream(namespace).file().fileConfig().init().name(taskName).sqlPath(sqlPath);
    }

    @Override public void start(String namespace, String taskName) throws Exception {
        SqlStreamBuilder.remoteSqlStream(namespace).file().fileConfig().init().name(taskName).start();
        System.out.println("start task success");
    }

    @Override public void stop(String namespace, String taskName) throws Exception {
        SqlStreamBuilder.remoteSqlStream(namespace).file().fileConfig().init().name(taskName).stop();
    }

    @Override public String list(String namespace) throws Exception {
        List<StreamsTask> streamsTaskList = SqlStreamBuilder.remoteSqlStream(namespace).file().fileConfig().init().list();
        StringBuilder builder = new StringBuilder();
        builder.append("namespace").append("      ").append("task_name").append("      ").append("state").append("\n");
        builder.append("--------------------------------------------------------------------------------------------").append("\n");
        for (StreamsTask streamsTask : streamsTaskList) {
            builder.append(namespace).append("      ").append(streamsTask.getConfigureName()).append("      ").append(streamsTask.getState()).append("\n");
        }
        return builder.toString();
    }

}
