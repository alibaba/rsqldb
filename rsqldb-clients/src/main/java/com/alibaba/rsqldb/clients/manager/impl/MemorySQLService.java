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
package com.alibaba.rsqldb.clients.manager.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.alibaba.rsqldb.clients.manager.ISQLService;
import com.alibaba.rsqldb.clients.manager.SQLForJob;

import com.google.auto.service.AutoService;
import org.apache.rocketmq.streams.common.model.ServiceName;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;

@AutoService(ISQLService.class)
@ServiceName("MEMORY")
public class MemorySQLService implements ISQLService {
    protected Map<String, List<SQLForJob>> allSQL = new HashMap<>();
    protected Map<String, SQLForJob> name2SQLForJob = new HashMap<>();

    @Override
    public void initFromProperties(Properties properties) {

    }

    @Override
    public List<SQLForJob> select(String namespace) {
        return allSQL.get(namespace);
    }

    @Override
    public SQLForJob select(String namespace, String name) {
        return name2SQLForJob.get(MapKeyUtil.createKey(namespace, name));
    }

    @Override
    public synchronized void saveOrUpdate(SQLForJob sqlForJob) {
        String namespace = sqlForJob.getNameSpace();
        if (StringUtil.isEmpty(namespace) || StringUtil.isEmpty(sqlForJob.getName())) {
            throw new RuntimeException("sqlForJob namespace or name can not empty");
        }
        List<SQLForJob> list = allSQL.get(namespace);
        if (list == null) {
            list = new ArrayList<>();
            allSQL.put(namespace, list);
        }
        if (!list.contains(sqlForJob)) {
            list.add(sqlForJob);
        }
        name2SQLForJob.put(MapKeyUtil.createKey(namespace, sqlForJob.getName()), sqlForJob);
    }

    @Override
    public synchronized void delete(String namespace, String name) {
        if (StringUtil.isEmpty(namespace) || StringUtil.isEmpty(name)) {
            throw new RuntimeException("sqlForJob namespace or name can not empty");
        }
        List<SQLForJob> list = allSQL.get(namespace);
        if (list != null) {
            List<SQLForJob> copy = new ArrayList<>();
            for (SQLForJob sqlForJob : list) {
                if (!sqlForJob.getName().equals(name)) {
                    copy.add(sqlForJob);
                }
            }
            allSQL.put(namespace, copy);
        }
        name2SQLForJob.remove(MapKeyUtil.createKey(namespace, name));

    }
}
