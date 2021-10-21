package com.alibaba.rsqldb.clients.sql;

import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.common.configure.ConfigureFileKey;
import org.apache.rocketmq.streams.configurable.ConfigurableComponent;

/**
 * can auto load sql and execute
 */
public class SQLStreamServer {
    protected volatile  boolean isStop=false;

    public void startServer(String... namespaces){
        String connectType=ComponentCreator.getProperties().getProperty(ConfigureFileKey.CONNECT_TYPE );
        if(!IConfigurableService.DEFAULT_SERVICE_NAME.equals(connectType )){
            throw new RuntimeException("server mode need configue db strategy");
        }
        if(namespaces==null){
            return;
        }
        for(String namespace:namespaces){
           ConfigurableComponent configurableComponent=ConfigurableComponent.getInstance(namespace);
           configurableComponent.startComponent(namespace);
        }
        while (!isStop){
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public boolean isStop() {
        return isStop;
    }

    public void setStop(boolean stop) {
        isStop = stop;
    }
}
