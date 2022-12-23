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
package com.alibaba.rsqldb.parser.model;


import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

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

    public Field(String content, String tableName, String fieldName, String asFieldName) {
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
    public int hashCode() {
        int total = 0;
        if (!StringUtils.isEmpty(tableName)) {
            total += tableName.hashCode();
        }

        if (!StringUtils.isEmpty(fieldName)) {
            total += fieldName.hashCode();
        }

        if (!StringUtils.isEmpty(asFieldName)) {
            total += asFieldName.hashCode();
        }

        return total;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Field)) {
            return false;
        }

        Field that = (Field) obj;

        return Objects.equals(this.tableName, that.tableName)
                && Objects.equals(this.fieldName, that.fieldName)
                && Objects.equals(this.asFieldName, that.asFieldName);
    }

    @Override
    public String toString() {
        return "Field{" +
                "tableName='" + tableName + '\'' +
                ", fieldName='" + fieldName + '\'' +
                ", asFieldName='" + asFieldName + '\'' +
                '}';
    }
}
