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
package com.alibaba.rsqldb.parser.parser.builder;

import com.alibaba.rsqldb.parser.parser.SQLParserContext;

import java.util.HashSet;
import java.util.Set;

public class TableNodeBuilder extends SelectSQLBuilder {

    @Override
    protected void build() {

    }

    @Override
    public Set<String> parseDependentTables() {
        Set<String> tables = new HashSet<>();
        tables.add(getTableName());
        return tables;
    }

    @Override
    public Set<String> getAllFieldNames() {

        return SQLParserContext.getInstance().get().get(getTableName());
    }



    @Override
    public String getFieldName(String fieldName) {
        String name = doAllFieldName(fieldName);
        if (name != null) {
            return name;
        }
        String tableName = getTableName();
        Set<String> fieldNames = SQLParserContext.getInstance().get().get(tableName);
        String asName = getAsName();
        int index = fieldName.indexOf(".");
        if (index == -1) {
            //if(fieldNames==null){
            //    System.out.println("");
            //}
            if (fieldNames == null) {
                return fieldName;
            }
            if (fieldNames.contains(fieldName)) {
                return fieldName;
            } else {
                return null;
            }
        }
        String ailasName = fieldName.substring(0, index);
        fieldName = fieldName.substring(index + 1);
        if (ailasName.equals(asName) && fieldNames.contains(fieldName)) {
            return fieldName;
        } else {
            return null;
        }
    }

    @Override
    public String getFieldName(String fieldName, boolean containsSelf) {
        return getFieldName(fieldName);
    }

}