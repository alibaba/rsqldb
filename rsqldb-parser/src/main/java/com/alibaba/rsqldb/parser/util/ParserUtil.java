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
package com.alibaba.rsqldb.parser.util;

import com.alibaba.rsqldb.parser.SqlParser;
import com.alibaba.rsqldb.parser.model.Operator;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Vocabulary;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class ParserUtil {
    private static final Logger logger = LoggerFactory.getLogger(ParserUtil.class);

    public static String getText(ParserRuleContext context) {
        if (context == null) {
            return null;
        }

        CharStream inputStream = context.start.getInputStream();
        int start = context.start.getStartIndex();
        int end = context.stop.getStopIndex();
        Interval interval = Interval.of(start, end);

        String text = inputStream.getText(interval);
        return removeQuoted(text);
    }


    private static String removeQuoted(String content) {
        if (StringUtils.isEmpty(content)) {
            return null;
        }

        if (content.startsWith("'") && content.endsWith("'")
                || content.startsWith("`") && content.endsWith("`")
                || content.startsWith("\"") && content.endsWith("\"")) {
            return content.substring(1, content.length() - 1);
        }

        return content;
    }

    public static String getLiteralText(TerminalNode terminalNode) {
        if (terminalNode == null) {
            return null;
        }

        String text = terminalNode.getText();
        text = text.substring(1, text.length() - 1);

        if (StringUtils.isEmpty(text)) {
            return null;
        }

        return text;
    }

    public static Operator getOperator(String operator) {
        if (StringUtils.isEmpty(operator)) {
            throw new IllegalArgumentException("operator is null");
        }

        for (Operator value : Operator.values()) {
            if (value.name().equalsIgnoreCase(operator)
                    || value.getSymbol().equalsIgnoreCase(operator)
                    || (value.getNickName() != null && value.getNickName().equalsIgnoreCase(operator))) {
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

    public static boolean isKeyWord(String targetIdentifier) {
        Vocabulary vocabulary = SqlParser.VOCABULARY;
        int nums = vocabulary.getMaxTokenType();

        for (int i = 0; i < nums; i++) {
            String keyWord = vocabulary.getSymbolicName(i);
            if (!StringUtils.isEmpty(keyWord) && keyWord.equalsIgnoreCase(targetIdentifier)) {
                logger.info("mismatched input:[{}] is a keyword:[{}], to be a variable is ok.", targetIdentifier, keyWord);
                return true;
            }
        }
        return false;
    }

}
