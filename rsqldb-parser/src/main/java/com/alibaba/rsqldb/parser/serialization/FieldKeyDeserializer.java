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
package com.alibaba.rsqldb.parser.serialization;

import com.alibaba.rsqldb.parser.model.Field;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.KeyDeserializer;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.streams.core.common.Constant;

import java.io.IOException;

public class FieldKeyDeserializer extends KeyDeserializer {

    @Override
    public Object deserializeKey(String key, DeserializationContext ctxt) throws IOException {
         if (StringUtils.isEmpty(key)) {
             return null;
         }

        String[] split = key.split(Constant.SPLIT);


        String[] contentField = split[0].split("=");
        String content = contentField[1];

        String[] tableNameField = split[1].split("=");
        String tableName = tableNameField[1];

        String[] fieldNameField = split[2].split("=");
        String fieldName = fieldNameField[1];

        String[] asFieldNameField = split[3].split("=");
        String asFieldName = asFieldNameField[1];

        return new Field(content, tableName, fieldName, asFieldName);
    }
}
