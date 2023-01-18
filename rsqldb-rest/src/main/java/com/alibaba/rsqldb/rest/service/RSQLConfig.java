/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
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

import org.apache.commons.lang3.StringUtils;


public class RSQLConfig {
    private String namesrvAddr = "127.0.0.1:9876";

    private String storage = "rocketmq";

    RSQLConfig() {
    }


    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }


    public String getStorage() {
        return storage;
    }

    public void setStorage(String storage) {
        if (!StringUtils.isBlank(storage)) {
            this.storage = storage.toLowerCase();
        }
    }
}
