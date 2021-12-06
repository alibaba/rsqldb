package com.alibaba.rsqldb.clients.sql;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.rocketmq.streams.client.strategy.Strategy;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.common.configure.ConfigureFileKey;
import org.apache.rocketmq.streams.configurable.ConfigurableComponent;

import com.alibaba.rsqldb.parser.entity.SqlTask;

/**
 * can execute sql directly can submit sql to server support sql assemble
 */
public class SQLStream {

    protected String namespace;
    protected String pipelineName;
    protected String sql;
    protected volatile boolean isStop = false;
    protected List<String> sameSourceSQL = new ArrayList<>();

    public SQLStream(String namespace, String pipelineName, String sql) {
        this.namespace = namespace;
        this.pipelineName = pipelineName;
        this.sql = sql;
    }

    public void start() {
        startSQL(false);
    }

    public void asyncStart() {
        startSQL(true);
    }

    protected void startSQL(boolean isAsync) {
        if (sql == null) {
            return;
        }
        SqlTask sqlTask = build(ConfigurableComponent.getInstance(namespace));
        sqlTask.startSQL();
        if (isAsync) {
            while (!isStop) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public SQLStream with(Strategy... strategies) {
        Properties properties = new Properties();
        for (Strategy strategy : strategies) {
            properties.putAll(strategy.getStrategyProperties());
        }
        ComponentCreator.createProperties(properties);
        return this;
    }

    public void addSameSourceSQL(String sql) {
        sameSourceSQL.add(sql);
    }

    public void submit(String serverUrl, String userName, String password, boolean isSubmitParent) {
        String[] propertys = new String[4];
        propertys[0] = ConfigureFileKey.CONNECT_TYPE + ":" + IConfigurableService.DEFAULT_SERVICE_NAME;
        propertys[1] = ConfigureFileKey.JDBC_URL + ":" + serverUrl;
        propertys[2] = ConfigureFileKey.JDBC_USERNAME + ":" + userName;
        propertys[3] = ConfigureFileKey.JDBC_PASSWORD + ":" + password;
        // if(isSubmitParent&&parent!=null){
        //     parent.submit(serverUrl,userName,password,false);
        // }
        ConfigurableComponent configurableComponent = ComponentCreator.getComponent(namespace, ConfigurableComponent.class, propertys);
        build(configurableComponent);
    }

    public SqlTask build(ConfigurableComponent configurableComponent) {
        SqlTask sqlTask = new SqlTask(namespace, pipelineName, sql);
        sqlTask.build(configurableComponent);
        if (sameSourceSQL != null && sameSourceSQL.size() > 0) {
            for (String sql : sameSourceSQL) {
                SqlTask builder = new SqlTask(namespace, pipelineName, sql);
                builder.build(configurableComponent);
            }
        }
        return sqlTask;
    }

}
