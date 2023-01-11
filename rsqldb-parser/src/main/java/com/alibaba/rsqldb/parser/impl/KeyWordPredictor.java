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
package com.alibaba.rsqldb.parser.impl;

import com.alibaba.rsqldb.parser.SqlParser;
import com.alibaba.rsqldb.parser.util.ParserUtil;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.RuleContext;
import org.antlr.v4.runtime.Vocabulary;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiFunction;
import java.util.regex.Pattern;

public class KeyWordPredictor implements BiFunction<String, RecognitionException, Boolean> {
    private static final Pattern MISMATCHED_PATTERN = Pattern.compile("mismatched input.*expecting.*");

    @Override
    public Boolean apply(String msg, RecognitionException exception) {
        //1.msg pattern is match
        if (StringUtils.isEmpty(msg) || exception == null) {
            return false;
        }
        if (!MISMATCHED_PATTERN.matcher(msg).matches()) {
            return false;
        }


        //2.ctx is an identifier
        RuleContext context = exception.getCtx();
        if (!(context instanceof SqlParser.IdentifierContext)) {
            return false;
        }

        String targetIdentifier = exception.getOffendingToken().getText();

        return ParserUtil.isKeyWord(targetIdentifier);
    }
}
