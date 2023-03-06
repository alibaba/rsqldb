/*
 * Copyright 1999-2023 Alibaba Group Holding Ltd.
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
package com.alibaba.rsqldb.rest.service;

import com.alibaba.rsqldb.common.RSQLConstant;
import com.alibaba.rsqldb.common.exception.RSQLServerException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

@Service
public class RSQLConfigBuilder {
    private static final Logger logger = LoggerFactory.getLogger(RSQLConfigBuilder.class);
    private static final String relativePath = "/distribution/conf/";
    private static final String configFile = "rsqldb.conf";
    private final Properties properties;

    private RSQLConfig config;

    public RSQLConfigBuilder() {
        properties = new Properties();

        String configPath = System.getProperty(RSQLConstant.CONFIG_PATH);
        loadConfig(configPath);
    }

    public synchronized RSQLConfig build() {
        if (config == null) {
            config = new RSQLConfig();
            properties2Object(this.properties, config);
        }

        return config;
    }

    public Properties getProperties() {
        return new Properties(properties);
    }

    private void loadConfig(String path) {
        try {
            if (StringUtils.isBlank(path)) {
                File file = new File("");
                path = file.getCanonicalFile() + relativePath + configFile;
            }
            logger.info("path of rsqldb.conf:{}", path);


            InputStream in = new BufferedInputStream(Files.newInputStream(Paths.get(path)));
            properties.load(in);

            in.close();
        } catch (Throwable e) {
            logger.error("load rsqldb.conf file error.", e);
            throw new RSQLServerException(e);
        }
    }

    private void properties2Object(final Properties p, final Object object) {
        Method[] methods = object.getClass().getMethods();
        for (Method method : methods) {
            String mn = method.getName();
            if (mn.startsWith("set")) {
                try {
                    String tmp = mn.substring(4);
                    String first = mn.substring(3, 4);

                    String key = first.toLowerCase() + tmp;
                    String property = p.getProperty(key);
                    if (property != null) {
                        Class<?>[] pt = method.getParameterTypes();
                        if (pt != null && pt.length > 0) {
                            String cn = pt[0].getSimpleName();
                            Object arg = null;
                            if (cn.equals("int") || cn.equals("Integer")) {
                                arg = Integer.parseInt(property);
                            } else if (cn.equals("long") || cn.equals("Long")) {
                                arg = Long.parseLong(property);
                            } else if (cn.equals("double") || cn.equals("Double")) {
                                arg = Double.parseDouble(property);
                            } else if (cn.equals("boolean") || cn.equals("Boolean")) {
                                arg = Boolean.parseBoolean(property);
                            } else if (cn.equals("float") || cn.equals("Float")) {
                                arg = Float.parseFloat(property);
                            } else if (cn.equals("String")) {
                                arg = property;
                            } else {
                                continue;
                            }
                            method.invoke(object, arg);
                        }
                    }
                } catch (Throwable ignored) {
                }
            }
        }
    }
}
