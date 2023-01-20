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
package com.alibaba.rsqldb.parser.impl;

import com.alibaba.rsqldb.common.exception.SyntaxErrorException;
import com.alibaba.rsqldb.parser.SqlParser;
import com.alibaba.rsqldb.parser.util.ParserUtil;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonToken;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.RuleContext;
import org.antlr.v4.runtime.misc.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultErrorListener extends BaseErrorListener {
    private static final Logger logger = LoggerFactory.getLogger(DefaultErrorListener.class);
    private final KeyWordPredictor predictor = new KeyWordPredictor();

    @Override
    public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol,
                            int line, int charPositionInLine, String msg, RecognitionException e) {
        if (predictor.apply(msg, e)) {
            return;
        }

        CharStream inputStream = ((CommonToken) offendingSymbol).getInputStream();
        String sql = inputStream.getText(Interval.of(0, 1000));

        logger.error("error when parse sql. position=(line:{}, char index:{}), error msg:{}, \n {}", line, charPositionInLine, msg, sql);
        if (e != null) {
            if (e.getOffendingToken() != null) {
                String targetIdentifier = e.getOffendingToken().getText();
                boolean keyWord = ParserUtil.isKeyWord(targetIdentifier);
                if (keyWord) {
                    logger.error("{} is a keyWord, change it to `{}`", targetIdentifier, targetIdentifier);
                }
            }
            throw new SyntaxErrorException(msg, e);
        } else {
            throw new SyntaxErrorException(msg);
        }
    }

}
