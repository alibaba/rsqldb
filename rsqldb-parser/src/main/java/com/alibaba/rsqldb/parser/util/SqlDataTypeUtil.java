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
package com.alibaba.rsqldb.parser.util;

import org.apache.rocketmq.streams.common.datatype.BooleanDataType;
import org.apache.rocketmq.streams.common.datatype.DataType;
import org.apache.rocketmq.streams.common.datatype.DateDataType;
import org.apache.rocketmq.streams.common.datatype.DoubleDataType;
import org.apache.rocketmq.streams.common.datatype.FloatDataType;
import org.apache.rocketmq.streams.common.datatype.IntDataType;
import org.apache.rocketmq.streams.common.datatype.LongDataType;
import org.apache.rocketmq.streams.common.datatype.StringDataType;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

import java.util.HashMap;
import java.util.Map;

public class SqlDataTypeUtil {

    private static Map<String, DataType> dataTypeMap = new HashMap<>();

    static {
        dataTypeMap.put("VARCHAR", new StringDataType());
        dataTypeMap.put("BIGINT", new LongDataType());
        dataTypeMap.put("TINYINT", new IntDataType());
        dataTypeMap.put("SMALLINT", new IntDataType());
        dataTypeMap.put("MEDIUMINT", new IntDataType());
        dataTypeMap.put("INT", new IntDataType());
        dataTypeMap.put("INTEGER", new IntDataType());
        dataTypeMap.put("FLOAT", new FloatDataType());
        dataTypeMap.put("DOUBLE", new DoubleDataType());
        dataTypeMap.put("DECIMAL", new DoubleDataType());
        dataTypeMap.put("DATETIME", new DateDataType());
        dataTypeMap.put("TIMESTAMP", new DateDataType());
        dataTypeMap.put("DATE", new DateDataType());
        dataTypeMap.put("TIME", new DateDataType());
        dataTypeMap.put("TINYTEXT", new StringDataType());
        dataTypeMap.put("TEXT", new StringDataType());
        dataTypeMap.put("MEDIUMTEXT", new StringDataType());
        dataTypeMap.put("LONGTEXT", new StringDataType());
        dataTypeMap.put("CHAR", new StringDataType());
        dataTypeMap.put("BOOLEAN", new BooleanDataType());
    }

    public static DataType covert(String sqlDataType) {
        sqlDataType = sqlDataType.toUpperCase();
        sqlDataType = FunctionUtils.getConstant(sqlDataType);
        return dataTypeMap.get(sqlDataType);
    }
}
