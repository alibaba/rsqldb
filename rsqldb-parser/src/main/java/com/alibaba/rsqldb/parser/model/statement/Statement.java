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

import com.alibaba.rsqldb.common.RSQLConstant;
import com.alibaba.rsqldb.common.exception.SyntaxErrorException;
import com.alibaba.rsqldb.parser.impl.BuildContext;
import com.alibaba.rsqldb.parser.model.Node;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.StringUtils;

import java.util.Iterator;
import java.util.Map;

public abstract class Statement extends Node {
    private String tableName;

    public Statement(String content, String tableName) {
        super(content);
        if (StringUtils.isEmpty(tableName)) {
            throw new SyntaxErrorException("table name is null.");
        }
        this.tableName = tableName;
    }

    public abstract BuildContext build(BuildContext context) throws Throwable;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /**
     * 从jsonNode中挑选select出的字段
     * @param jsonNode
     * @param fieldNames
     * @return
     */
    protected JsonNode map(JsonNode jsonNode, Map<String/*fieldName*/, String/*newName*/> fieldNames) {
        if (jsonNode == null) {
            return null;
        }

        if (fieldNames == null || fieldNames.size() == 0) {
            throw new SyntaxErrorException("select field is null. sql=" + this.getContent());
        }

        if (fieldNames.size() == 1
                && fieldNames.keySet().toArray()[0].equals(RSQLConstant.STAR)) {
            //全选
        } else {
            Iterator<Map.Entry<String, JsonNode>> entryIterator = jsonNode.fields();
            while (entryIterator.hasNext()) {
                Map.Entry<String, JsonNode> next = entryIterator.next();
                if (!fieldNames.containsKey(next.getKey())) {
                    entryIterator.remove();
                } else {
                    //todo
                    ObjectNode objectNode = (ObjectNode) jsonNode;
                    objectNode.replace(fieldNames.get(next.getKey()), next.getValue());
                }
            }
        }

        return jsonNode;
    }


}
