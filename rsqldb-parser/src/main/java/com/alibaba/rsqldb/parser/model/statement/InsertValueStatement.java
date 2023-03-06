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
package com.alibaba.rsqldb.parser.model.statement;

import com.alibaba.rsqldb.common.SerializeType;
import com.alibaba.rsqldb.common.exception.SyntaxErrorException;
import com.alibaba.rsqldb.parser.serialization.SerializeTypeContainer;
import com.alibaba.rsqldb.parser.serialization.Serializer;
import com.alibaba.rsqldb.parser.impl.BuildContext;
import com.alibaba.rsqldb.parser.model.ColumnValue;
import com.alibaba.rsqldb.parser.model.FieldType;
import com.alibaba.rsqldb.parser.model.baseType.BooleanType;
import com.alibaba.rsqldb.parser.model.baseType.Literal;
import com.alibaba.rsqldb.parser.model.baseType.NumberType;
import com.alibaba.rsqldb.parser.model.baseType.StringType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.streams.core.util.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ColumnValue：
 * 如果fieldName和fieldType都为空，是全表插入，则value的类型由create table决定
 * INSERT INTO `purchaser_dim` VALUES (1,'tom','male','16');
 *
 * 如果fieldName和fieldType不为空，是选择字段插入，需要与create table创建的表对比字段类型是否一致
 *String sql = "INSERT INTO Customers (CustomerName, ContactName, Address, City, PostalCode, Country)\n" +
 *                 "VALUES ('Cardinal', 'Tom B. Erichsen', 'Skagen 21', 'Stavanger', '4006', 'Norway');";
 *
 * 没有source，只有sink
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class InsertValueStatement extends Statement {
    private static final String template = "insert sql=[%s], create table sql=[%s]";
    //columns中顺序就是insert语句中值的顺序：INSERT INTO `purchaser_dim` VALUES (1,'tom','male','16');
    private List<ColumnValue> columns = new ArrayList<>();

    @JsonCreator
    public InsertValueStatement(@JsonProperty("content") String content, @JsonProperty("tableName") String tableName,
                                @JsonProperty("columns") List<ColumnValue> columns) {
        super(content, tableName);
        this.columns = columns;
    }

    public List<ColumnValue> getColumns() {
        return columns;
    }

    public void setColumns(List<ColumnValue> columns) {
        this.columns = columns;
    }

    @Override
    public BuildContext build(BuildContext context) throws Throwable {
        //columns是否是所有列，是的话挨次插入
        CreateTableStatement createTableStatement = context.getCreateTableStatement();
        validateAndAdd(createTableStatement);

        HashMap<String, Literal<?>> map = new HashMap<>();
        //加上插入的kv
        for (ColumnValue column : columns) {
            String fieldName = column.getFieldName();
            Literal<?> literal = column.getValue();
            FieldType fieldType = column.getFieldType();

            //createTable指定的fieldType 与插入的数据类型一致
            Class<?> clazz = fieldType.getClazz();
            if (clazz == Boolean.class && !(literal instanceof BooleanType)) {
                throw new SyntaxErrorException("field type is boolean, but insert value type is not boolean. "
                        + String.format(template, this.getContent(), createTableStatement.getContent()));
            } else if (clazz == Number.class && !(literal instanceof NumberType)) {
                throw new SyntaxErrorException("field type is Number, but insert value type is not Number. "
                        + String.format(template, this.getContent(), createTableStatement.getContent()));
            } else if (clazz == String.class && !(literal instanceof StringType)) {
                throw new SyntaxErrorException("field type is String, but insert value type is not String. "
                        + String.format(template, this.getContent(), createTableStatement.getContent()));
            }

            map.put(fieldName, literal);
        }

        //还要加上没有插入的fieldName 和null
        for (String fieldName : createTableStatement.getColumns().getFields()) {
            if (!map.containsKey(fieldName)) {
                map.put(fieldName, null);
            }
        }

        HashMap<String, Object> fieldAndValue = new HashMap<>();
        map.forEach((s, literal) -> fieldAndValue.put(s, literal.result()));

        SerializeType type = context.getCreateTableStatement().getSerializeType();
        Serializer serializer = SerializeTypeContainer.getSerializer(type);

        //todo 应该怎么才能满足使用者自定义输出的要求，目前object -> json string -> byte[]
        String str = context.getObjectMapper().writeValueAsString(fieldAndValue);

        byte[] body = serializer.serialize(str);

        context.setInsertValueData(body);
        return context;
    }

    /**
     * 必须是在构建流处理任务是判断是否有效，不能在解析时判断，因为需要得到createTable信息
     */
    private void validateAndAdd(CreateTableStatement createTableStatement) {

        List<Pair<String, FieldType>> holder = createTableStatement.getColumns().getHolder();

        Map<String, FieldType> fieldName2Type = new HashMap<>();
        for (Pair<String, FieldType> pair : holder) {
            fieldName2Type.put(pair.getKey(), pair.getValue());
        }

        int emptyNum = 0;
        int index = 0;

        for (ColumnValue column : columns) {
            String fieldName = column.getFieldName();
            FieldType fieldType = column.getFieldType();

            if (!StringUtils.isEmpty(fieldName) && fieldType == null) {
                FieldType type = fieldName2Type.get(fieldName);
                if (type == null) {
                    throw new SyntaxErrorException("fieldName:[" + fieldName + "] from insert sql not in create sql. "
                            + String.format(template, this.getContent(), createTableStatement.getContent()));
                }
                column.setFieldType(type);
                continue;
            }

            if (StringUtils.isEmpty(fieldName) && fieldType == null) {
                //INSERT INTO `purchaser_dim` VALUES (1,'tom','male','16');场景，插入字段个数必须和表字段个数一致
                emptyNum++;
                //columns中字段顺序就是createTable时Columns中list的顺序，直接赋值
                Pair<String, FieldType> typePair = createTableStatement.getColumns().getHolder().get(index++);
                column.setFieldName(typePair.getKey());
                column.setFieldType(typePair.getValue());
            }
        }

        if (emptyNum != 0 && (emptyNum != columns.size() || columns.size() != fieldName2Type.size())) {
            throw new SyntaxErrorException("column num in insert sql not equals the num in create sql. "
                    + String.format(template, this.getContent(), createTableStatement.getContent()));
        }
    }


    @Override
    public String toString() {
        return "Insert{" +
                "tableName='" + this.getTableName() + '\'' +
                ", columns=" + columns +
                '}';
    }
}
