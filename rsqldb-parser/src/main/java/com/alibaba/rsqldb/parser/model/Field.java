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
package com.alibaba.rsqldb.parser.model;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.streams.core.common.Constant;

import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Field extends Node {
    //可能为null
    private String tableName;
    private String fieldName;
    private String asFieldName;


    public Field(String content, String fieldName) {
        super(content);
        this.fieldName = fieldName;
    }


    public Field(String content, String tableName, String fieldName) {
        super(content);
        this.tableName = tableName;
        this.fieldName = fieldName;
    }

    @JsonCreator
    public Field(@JsonProperty("content") String content, @JsonProperty("tableName") String tableName,
                 @JsonProperty("fieldName") String fieldName, @JsonProperty("asFieldName") String asFieldName) {
        super(content);
        this.tableName = tableName;
        this.fieldName = fieldName;
        this.asFieldName = asFieldName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getAsFieldName() {
        return asFieldName;
    }

    public void setAsFieldName(String asFieldName) {
        this.asFieldName = asFieldName;
    }


    @Override
    public String toString() {
        StringBuffer buffer = new StringBuffer();
        buffer.append("content").append("=").append(this.getContent()).append(Constant.SPLIT)
                .append("tableName").append("=").append(convert(tableName)).append(Constant.SPLIT)
                .append("fieldName").append("=").append(convert(fieldName)).append(Constant.SPLIT)
                .append("asFieldName").append("=").append(convert(asFieldName));

        return buffer.toString();
    }

    private String convert(String value) {
        return StringUtils.isNotBlank(value) ? value : "";
    }
}
