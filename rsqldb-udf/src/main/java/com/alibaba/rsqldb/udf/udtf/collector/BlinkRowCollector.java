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
package com.alibaba.rsqldb.udf.udtf.collector;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.rsqldb.udf.BlinkDataType;
import com.alibaba.rsqldb.udf.udtf.BlinkUDTFScript;

import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.datatype.*;
import org.apache.rocketmq.streams.common.datatype.DataType;
import org.apache.rocketmq.streams.common.datatype.StringDataType;
import org.apache.rocketmq.streams.common.model.ThreadContext;
import org.apache.rocketmq.streams.script.context.FunctionContext;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.types.DataTypes;
import org.apache.flink.table.types.RowType;
import org.apache.flink.table.types.TypeInfoWrappedType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.rocketmq.streams.script.function.model.FunctionType;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class BlinkRowCollector implements Collector<Row> {
    private static final String RESULT_TYPE_METHOD_NAME = "getResultType";//获取返回类型的方法名

    protected transient BlinkUDTFScript target;
    private transient List<DataType> dataTypes;

    public BlinkRowCollector(BlinkUDTFScript target) {
        this.target = target;
    }

    @Override
    public void collect(Row row) {
        FunctionContext context = (FunctionContext)loadContext();
        int size = row.getArity();
        if (dataTypes == null) {
            this.dataTypes = loadResultType(row);
        }
        IMessage message = context.getMessage();
        final JSONObject jsonObject = message.getMessageBody();
        JSONObject newMessage = new JSONObject();
        newMessage.putAll(jsonObject);
        for (int i = 0; i < size; i++) {
            newMessage.put(FunctionType.UDTF.getName()  + i,
                dataTypes.get(i).getData(row.getField(i).toString()));
        }
        Message msg = new Message(newMessage);
        msg.setHeader(message.getHeader().copy());
        ;
        context.addSplitMessages(msg);
        context.openSplitModel();
    }

    /**
     * 执行udtf中的getResultType方法获取，拆分数据的类型
     *
     * @return
     */
    private List<DataType> loadResultType(Row row) {
        Object object = target.getInstance();
        try {
            Method method =
                object.getClass().getMethod(RESULT_TYPE_METHOD_NAME, new Class[] {Object[].class, Class[].class});
            org.apache.flink.table.types.DataType dataType = null;
            try {
                Class[] classes = new Class[row.getArity()];
                Object[] objects = new Object[row.getArity()];
                for (int i = 0; i < row.getArity(); i++) {
                    Object object1 = row.getField(i);
                    objects[i] = object1;
                    if (object1 != null) {
                        classes[i] = object1.getClass();
                    }
                }
                dataType = (org.apache.flink.table.types.DataType)method.invoke(object, objects, classes);
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            }
            if (RowType.class.isInstance(dataType)) {
                return convertRowType((RowType)dataType);
            }

            TypeInfoWrappedType typeInfoWrappedType = (TypeInfoWrappedType)dataType;
            TypeInformation typeInformation = typeInfoWrappedType.getTypeInfo();

            Map<String, TypeInformation<?>> typeInformationMap = typeInformation.getGenericParameters();

            TypeInformation[] typeInformations = new TypeInformation[typeInformationMap.size()];
            Iterator<TypeInformation<?>> it = typeInformationMap.values().iterator();
            int i = 0;
            while (it.hasNext()) {
                typeInformations[i] = it.next();
                i++;
            }
            return convert(typeInformations);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("load result type error", e);
        }
    }

    protected List<DataType> convertRowType(RowType rowType) {
        List<DataType> dataTypes = new ArrayList<>();
        org.apache.flink.table.types.DataType[] types = rowType.getFieldTypes();
        for (org.apache.flink.table.types.DataType dataType : types) {
            if (DataTypes.STRING.equals(dataType)) {
                dataTypes.add(new StringDataType());
            } else if (DataTypes.BOOLEAN.equals(dataType)) {
                dataTypes.add(new BooleanDataType());
            } else if (DataTypes.INT.equals(dataType)) {
                dataTypes.add(new IntDataType());
            } else if (DataTypes.LONG.equals(dataType)) {
                dataTypes.add(new LongDataType());
            } else if (DataTypes.DOUBLE.equals(dataType)) {
                dataTypes.add(new DoubleDataType());
            } else if (DataTypes.FLOAT.equals(dataType)) {
                dataTypes.add(new FloatDataType());
            } else if (DataTypes.DATE.equals(dataType) || DataTypes.TIME.equals(dataType) || DataTypes.TIMESTAMP.equals(dataType)) {
                dataTypes.add(new DateDataType());
            } else if (DataTypes.SHORT.equals(dataType)) {
                dataTypes.add(new ShortDataType());
            } else if (DataTypes.BYTE.equals(dataType)) {
                dataTypes.add(new ByteDataType());
            } else {
                throw new RuntimeException("can not support this datatype from flink " + dataType.toString());
            }

        }
        return dataTypes;
    }

    private List<DataType> convert(TypeInformation[] typeInformations) {
        List<DataType> dataTypes = new ArrayList<>();
        for (TypeInformation typeInformation : typeInformations) {
            dataTypes.add(BlinkDataType.convertTypeInformation(typeInformation));
        }
        return dataTypes;
    }

    private AbstractContext loadContext() {
        ThreadContext threadContext = ThreadContext.getInstance();
        return threadContext.get();
    }

    @Override
    public void close() {

    }
}
