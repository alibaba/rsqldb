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

import com.alibaba.rsqldb.common.exception.SyntaxErrorException;
import com.alibaba.rsqldb.parser.model.Field;
import com.alibaba.rsqldb.parser.model.statement.query.WindowInfo;

import java.util.ArrayList;
import java.util.List;

public class Validator {

    //校验sql中定义在select字段上的window与定义在groupBy中的window一致,校验Type, Field、slide、size字段
    //sql中必须在要有一个windowInfos中FirstWordInSQL是WINDOW
    public static void window(List<WindowInfo> windowInfos) {
        if (windowInfos == null || windowInfos.size() == 0) {
            return;
        }
        if (windowInfos.size() > 3) {
            throw new SyntaxErrorException("window sql num large than 3.");
        }

        List<WindowInfo.WindowType> windowTypes = new ArrayList<>();
        List<Field> windowFields = new ArrayList<>();
        List<Long> windowSlide = new ArrayList<>();
        List<Long> windowSize = new ArrayList<>();

        int windowInGroupBy = 0;
        for (WindowInfo info : windowInfos) {
            //FirstWordInSQL
            if (info.getFirstWordInSQL().equals(WindowInfo.FirstWordInSQL.WINDOW)) {
                windowInGroupBy++;
            }

            WindowInfo.WindowType newType = info.getType();
            for (WindowInfo.WindowType old : windowTypes) {
                if (newType != old) {
                    throw new SyntaxErrorException("window type not same.");
                }
            }
            windowTypes.add(newType);

            Field newTimeField = info.getTimeField();
            for (Field old : windowFields) {
                if (!newTimeField.getFieldName().equals(old.getFieldName())) {
                    throw new SyntaxErrorException("window time field not same.");
                }
            }
            windowFields.add(newTimeField);

            long newOne = info.getSlide();
            for (Long old : windowSlide) {
                if (newOne != old) {
                    throw new SyntaxErrorException("window slide not same.");
                }
            }
            windowSlide.add(newOne);

            long newSize = info.getSize();
            for (Long old : windowSize) {
                if (newSize != old) {
                    throw new SyntaxErrorException("window size not same.");
                }
            }
            windowSize.add(newSize);
        }

        if (windowInGroupBy != 1) {
            throw new SyntaxErrorException("window sentence are illegal.");
        }
    }
}
