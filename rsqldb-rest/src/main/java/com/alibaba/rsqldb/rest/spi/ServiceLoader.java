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
package com.alibaba.rsqldb.rest.spi;


import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;

@Service
public class ServiceLoader {
    private static final String PREFIX = "META-INF/rsqldb/";

    @SuppressWarnings("unchecked")
    public <T> T load(Class<T> clazz, String name) throws Exception {
        String fullName = PREFIX + clazz.getName();
        Enumeration<URL> enumeration = ClassLoader.getSystemResources(fullName);

        String serviceClassName = null;
        while (enumeration.hasMoreElements()) {
            URL url = enumeration.nextElement();

            try (InputStream inputStream = url.openStream()) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));

                String item = null;
                while ((item = reader.readLine()) != null) {
                    String[] pair = item.split("=");
                    if (name.equalsIgnoreCase(pair[0])) {
                        if (serviceClassName == null) {
                            serviceClassName = pair[1];
                        } else {
                            throw new IllegalStateException("repeated name: " + name);
                        }
                    }
                }
            }
        }

        Class<T> temp = (Class<T>) Class.forName(serviceClassName);

        return temp.newInstance();
    }

}
