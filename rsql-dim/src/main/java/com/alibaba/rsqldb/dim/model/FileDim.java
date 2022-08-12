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
package com.alibaba.rsqldb.dim.model;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.cache.compress.AbstractMemoryTable;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.common.utils.FileUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileDim extends AbstractDim {

    private static final Log LOG = LogFactory.getLog(FileDim.class);

    protected String filePath;

    @Override
    protected void loadData2Memory(AbstractMemoryTable tableCompress) {
        List<String> rows = FileUtil.loadFileLine(filePath);
        if (rows == null) {
            throw new RuntimeException("there is no dim data from file " + filePath);
        }
        if (CollectionUtil.isEmpty(rows)) {
            LOG.warn("there is no dim data from file " + filePath);
            return;
        }
        for (String row : rows) {
            JSONObject jsonObject = JSON.parseObject(row);
            Map<String, Object> values = new HashMap<>();
            for (String key : jsonObject.keySet()) {
                values.put(key, jsonObject.getString(key));
            }
            tableCompress.addRow(values);
        }
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }
}
