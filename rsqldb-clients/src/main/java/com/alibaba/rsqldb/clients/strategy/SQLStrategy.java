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
package com.alibaba.rsqldb.clients.strategy;

import java.util.Properties;
import org.apache.rocketmq.streams.client.strategy.Strategy;
import org.apache.rocketmq.streams.common.classloader.IsolationClassLoader;
import org.apache.rocketmq.streams.common.component.AbstractComponent;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.script.ScriptComponent;
import com.alibaba.rsqldb.parser.parser.builder.BlinkUDFScan;

public class  SQLStrategy implements Strategy {

    private final Properties properties;

    private SQLStrategy(Long pollingTime) {
        properties = new Properties();
        properties.put(AbstractComponent.CONNECT_TYPE, IConfigurableService.MEMORY_SERVICE_NAME);
        properties.put(AbstractComponent.POLLING_TIME, pollingTime + "");
    }

    private SQLStrategy(String filePath, Long pollingTime) {
        properties = new Properties();
        properties.put(AbstractComponent.CONNECT_TYPE, IConfigurableService.FILE_SERVICE_NAME);
        properties.put(IConfigurableService.FILE_PATH_NAME, filePath);
        properties.put(AbstractComponent.POLLING_TIME, pollingTime + "");
    }

    private SQLStrategy(String url, String username, String password, Long pollingTime) {
        properties = new Properties();
        properties.put(AbstractComponent.JDBC_DRIVER, AbstractComponent.DEFAULT_JDBC_DRIVER);
        properties.put(AbstractComponent.JDBC_URL, url);
        properties.put(AbstractComponent.JDBC_USERNAME, username);
        properties.put(AbstractComponent.JDBC_PASSWORD, password);
        properties.put(AbstractComponent.JDBC_TABLE_NAME, AbstractComponent.DEFAULT_JDBC_TABLE_NAME);
        properties.put(AbstractComponent.POLLING_TIME, pollingTime + "");
        properties.put(AbstractComponent.CONNECT_TYPE, IConfigurableService.DEFAULT_SERVICE_NAME);
    }

    @Override
    public Properties getStrategyProperties() {
        return this.properties;
    }

    public static Strategy db(String url, String username, String password, Long pollingTime) {
        return new SQLStrategy(url, username, password, pollingTime);
    }

    public static Strategy file(String filePath, Long pollingTime) {
        return new SQLStrategy(filePath, pollingTime);
    }

    public SQLStrategy configBlinkUDF(String dir,String packageName){
        BlinkUDFScan.getInstance().registerBlinkUDF(dir,packageName);
        return this;
    }

    public SQLStrategy configJar(String dir,String className,String methodName){
        BlinkUDFScan.getInstance().registerJarUDF(dir,className,methodName);
        return this;
    }

    public SQLStrategy configFunction(String dir,String packageName){
        ScriptComponent.getInstance().getFunctionService().scanClassDir(dir,packageName,new IsolationClassLoader(dir));
        return this;
    }

}
