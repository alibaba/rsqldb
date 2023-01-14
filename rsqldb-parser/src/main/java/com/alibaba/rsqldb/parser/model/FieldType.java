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

import org.apache.commons.lang3.StringUtils;

public enum FieldType {
    INT("int", Number.class),
    BIGINT("bigint", Number.class),
    VARCHAR("varchar", String.class),
    TIMESTAMP("timestamp", Number.class);

    //type 与Literal中的T对应
    private final String type;
    private final Class<?> clazz;

    FieldType(String type, Class<?> clazz) {
        this.type = type;
        this.clazz = clazz;
    }

    public static FieldType getByType(String type) {
        if (StringUtils.isEmpty(type)) {
            return null;
        }

        if (INT.type.equalsIgnoreCase(type)) {
            return INT;
        }

        if (BIGINT.type.equalsIgnoreCase(type)) {
            return BIGINT;
        }

        if (VARCHAR.type.equalsIgnoreCase(type)) {
            return VARCHAR;
        }

        if (TIMESTAMP.type.equalsIgnoreCase(type)) {
            return TIMESTAMP;
        }

        throw new IllegalArgumentException("unrecognizable type: " + type);
    }

    public Class<?> getClazz() {
        return clazz;
    }

    @Override
    public String toString() {
        return type;
    }
}
