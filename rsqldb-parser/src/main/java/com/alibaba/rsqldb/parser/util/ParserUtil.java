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

import com.alibaba.rsqldb.parser.pojo.Operator;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.misc.Interval;
import org.apache.commons.lang3.StringUtils;

import java.util.concurrent.TimeUnit;

public class ParserUtil {
    public static String getText(ParserRuleContext context) {
        if (context == null) {
            return null;
        }

        CharStream inputStream = context.start.getInputStream();
        int start = context.start.getStartIndex();
        int end = context.stop.getStopIndex();
        Interval interval = Interval.of(start, end);
        return inputStream.getText(interval);
    }

    public static Operator getOperator(String operator) {
        if (StringUtils.isEmpty(operator)) {
            throw new IllegalArgumentException("operator is null");
        }

        for (Operator value : Operator.values()) {
            if (value.name().equalsIgnoreCase(operator)) {
                return value;
            }
        }

        throw new IllegalArgumentException("unrecognized operator: " + operator);
    }

    public static TimeUnit getTimeUnit(String unit) {
        if (StringUtils.isEmpty(unit)) {
            return null;
        }

        if (!unit.endsWith("s")) {
            unit = unit + "s";
        }
        return TimeUnit.valueOf(unit.toUpperCase());
    }

    public static void main(String[] args) {
        TimeUnit days = getTimeUnit("MILLISECOND");
        System.out.println(days);
    }
}
