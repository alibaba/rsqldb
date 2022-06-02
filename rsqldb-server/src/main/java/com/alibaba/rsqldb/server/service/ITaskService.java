package com.alibaba.rsqldb.server.service;

public interface ITaskService {
    /**
     * 完成实时任务的提交
     *
     * @param namespace 命名空间
     * @param taskName  任务名称
     * @param sql       任务sql
     * @throws Exception 异常
     */
    void submit(String namespace, String taskName, String sql) throws Exception;

    /**
     * 完成实时任务的提交
     * @param namespace 命名空间
     * @param taskName 任务名称
     * @param sqlPath 任务文件路径
     * @throws Exception 异常
     */
    void submitFile(String namespace, String taskName, String sqlPath) throws Exception;

    /**
     * 启动任务
     *
     * @param namespace 命名空间
     * @param taskName  任务名称
     * @throws Exception 异常
     */
    void start(String namespace, String taskName) throws Exception;

    /**
     * 停止任务
     *
     * @param namespace 命名空间
     * @param taskName  任务名称
     * @throws Exception 异常
     */
    void stop(String namespace, String taskName) throws Exception;

    /**
     * 列出所有任务
     *
     * @param namespace 命名空间
     * @return 任务列表
     * @throws Exception 异常
     */
    String list(String namespace) throws Exception;

}
