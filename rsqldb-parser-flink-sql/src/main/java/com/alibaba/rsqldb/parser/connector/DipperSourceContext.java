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
package com.alibaba.rsqldb.parser.connector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSONObject;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.rocketmq.streams.common.channel.source.AbstractSingleSplitSource;
import org.apache.rocketmq.streams.common.datatype.DataType;
import org.apache.rocketmq.streams.common.datatype.ListDataType;
import org.apache.rocketmq.streams.common.datatype.MapDataType;
import org.apache.rocketmq.streams.common.datatype.SetDataType;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.metadata.MetaDataField;
import org.apache.rocketmq.streams.connectors.reader.AbstractFlinkSplitReader;

public class DipperSourceContext implements SourceFunction.SourceContext<RowData> {
    protected AbstractSingleSplitSource source;
    protected AbstractFlinkSplitReader reader;

    public DipperSourceContext(AbstractSingleSplitSource source) {
        this.source = source;
    }

    public DipperSourceContext(AbstractFlinkSplitReader reader) {
        this.reader = reader;
    }

    @Override
    public void collect(RowData data) {
        MetaData metaData = source.getMetaData();
        List<MetaDataField> metaDataFields = metaData.getMetaDataFields();
        JSONObject row = new JSONObject();
        int index = 0;
        for (MetaDataField metaDataField : metaDataFields) {
            String fieldName = metaDataField.getFieldName();
            DataType dataType = metaDataField.getDataType();
            String dataTypeName = dataType.getDataTypeName();
            if ("int".equals(dataTypeName)) {
                row.put(fieldName, data.getInt(index));
            } else if ("long".equals(dataTypeName)) {
                row.put(fieldName, data.getLong(index));
            } else if ("double".equals(dataTypeName)) {
                row.put(fieldName, data.getDouble(index));
            } else if ("short".equals(dataTypeName)) {
                row.put(fieldName, data.getShort(index));
            } else if ("float".equals(dataTypeName)) {
                row.put(fieldName, data.getFloat(index));
            } else if ("boolean".equals(dataTypeName)) {
                row.put(fieldName, data.getBoolean(index));
            } else if ("string".equals(dataTypeName)) {
                row.put(fieldName, data.getString(index).toString());
            } else if ("date".equals(dataTypeName)) {
                row.put(fieldName, data.getTimestamp(index, 1));
            } else if ("byte".equals(dataTypeName)) {
                row.put(fieldName, data.getByte(index));
            } else if ("list".equals(dataTypeName)) {
                ListDataType listDataType = (ListDataType)dataType;
                List<?> list = getListFromRowArray(data.getArray(index), listDataType.getParadigmType());
                row.put(fieldName, list);
            } else if ("set".equals(dataTypeName)) {
                SetDataType setDataType = (SetDataType)dataType;
                List<?> list = getListFromRowArray(data.getArray(index), setDataType.getParadigmType());
                row.put(fieldName, new HashSet<>(list));
            } else if ("map".equals(dataTypeName)) {
                MapDataType mapDataType = (MapDataType)dataType;
                MapData mapData = data.getMap(index);
                List<?> keyList = getListFromRowArray(mapData.keyArray(), mapDataType.getKeyParadigmType());
                List<?> valueList = getListFromRowArray(mapData.valueArray(), mapDataType.getValueParadigmType());
                Map<Object, Object> map = new HashMap<>();
                for (int i = 0; i < keyList.size(); i++) {
                    map.put(keyList.get(i), valueList.get(i));
                }
                row.put(fieldName, map);
            }
            index++;
        }
        if (source != null) {
            source.doReceiveMessage(row);
        }
        if (reader != null) {
            reader.collect(row);
        }

    }

    private List<Object> getListFromRowArray(ArrayData array, DataType type) {
        List<Object> list = new ArrayList<>();
        for (int i = 0; i < array.size(); i++) {
            String str = array.getString(i).toString();
            list.add(type.getData(str));
        }
        return list;
    }

    @Override
    public void collectWithTimestamp(RowData data, long l) {

    }

    @Override
    public void emitWatermark(Watermark watermark) {

    }

    @Override
    public void markAsTemporarilyIdle() {

    }

    @Override
    public Object getCheckpointLock() {
        return null;
    }

    @Override
    public void close() {

    }

}
