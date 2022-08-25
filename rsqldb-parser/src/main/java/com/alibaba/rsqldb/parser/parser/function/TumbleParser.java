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
package com.alibaba.rsqldb.parser.parser.function;

import com.alibaba.rsqldb.parser.parser.builder.SelectSQLBuilder;
import com.alibaba.rsqldb.parser.parser.builder.WindowBuilder;
import com.alibaba.rsqldb.parser.parser.result.IParseResult;
import com.alibaba.rsqldb.parser.parser.result.VarParseResult;
import com.alibaba.rsqldb.parser.parser.sqlnode.AbstractSelectNodeParser;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlIntervalLiteral.IntervalValue;
import org.apache.calcite.sql.SqlNode;
import org.apache.rocketmq.streams.window.operator.AbstractWindow;

public class TumbleParser extends AbstractSelectNodeParser<SqlBasicCall> {

    @Override
    public IParseResult parse(SelectSQLBuilder builder, SqlBasicCall sqlBasicCall) {
        SqlNode[] operands = sqlBasicCall.getOperands();
        IParseResult fieldName = parseSqlNode(builder, operands[0]);
        SqlIntervalLiteral sqlIntervalLiteral = (SqlIntervalLiteral) operands[1];
        WindowBuilder windowBuilder = new WindowBuilder();
        windowBuilder.setType(AbstractWindow.TUMBLE_WINDOW);
        windowBuilder.setOwner(builder);
        setWindowParameter(true, windowBuilder, sqlIntervalLiteral);
        setWindowParameter(false, windowBuilder, sqlIntervalLiteral);
        windowBuilder.setTimeFieldName(fieldName.getReturnValue());
        builder.setWindowBuilder(windowBuilder);
        return new VarParseResult(null);
    }

    public static void setWindowParameter(boolean isSize, WindowBuilder builder, SqlIntervalLiteral intervalLiteral) {
        IntervalValue intervalValue = (IntervalValue) intervalLiteral.getValue();
        TimeUnit unit = intervalValue.getIntervalQualifier().getUnit();
        int interval = -1;
        try {
            interval = Integer.valueOf(intervalValue.getIntervalLiteral());
        } catch (Exception e) {
        }
        if (-1 == interval) {
            int coefficient = getDiff2Minute(unit);
            if (isSize) {
                builder.setSizeVariable(intervalValue.getIntervalLiteral());
                builder.setSizeAdjust(coefficient);
            } else {
                builder.setSlideVariable(intervalValue.getIntervalLiteral());
                builder.setSlideAdjust(coefficient);
            }
        } else {
            int windowStep = convert2Minute(interval, unit);
            if (isSize) {
                builder.setSize(windowStep);
            } else {
                builder.setSlide(windowStep);
            }
        }
    }

    /**
     * 节点转化成window 的滚动周期或滑动步长
     *
     * @param sqlIntervalLiteral
     * @return
     */
    protected int getWindowPeriod(SqlIntervalLiteral sqlIntervalLiteral) {
        IntervalValue intervalValue = (IntervalValue) sqlIntervalLiteral.getValue();
        //TODO default value
        int interval = 10;
        try {
            interval = Integer.parseInt(intervalValue.getIntervalLiteral());
        } catch (Exception e) {

        }

        TimeUnit unit = intervalValue.getIntervalQualifier().getUnit();
        return convert2Minute(interval, unit);
    }

    protected static int getDiff2Minute(TimeUnit timeUnit) {
        switch (timeUnit) {
            case SECOND:
                return 1;
            case MINUTE:
                return 60;
            case HOUR:
                return 60 * 60;
            case DAY:
                return 24 * 60 * 60;
            default:
                throw new RuntimeException("can not this time unit :" + timeUnit.toString()
                    + ", support second,minute,houre,day, millsecond");
        }
    }

    /**
     * 根据单位转化值为分钟的值
     *
     * @param interval
     * @param timeUnit
     * @return
     */
    protected static int convert2Minute(int interval, TimeUnit timeUnit) {
        int tumblePeriod = interval;
        if (timeUnit != null) {
            if (TimeUnit.SECOND == timeUnit) {
                tumblePeriod = interval;
            } else if (TimeUnit.MINUTE == timeUnit) {
                tumblePeriod = interval * 60;
            } else if (TimeUnit.HOUR == timeUnit) {
                tumblePeriod = interval * 60 * 60;
            } else if (TimeUnit.DAY == timeUnit) {
                tumblePeriod = interval * 24 * 60 * 60;
            } else {
                throw new RuntimeException("can not this time unit :" + timeUnit + ", support second,minute,houre,day, millsecond");
            }
        }
        return tumblePeriod;
    }

    @Override
    public boolean support(Object sqlNode) {
        if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
            if (sqlBasicCall.getOperator().getName().toLowerCase().equals("tumble")) {
                return true;
            }
        }
        return false;
    }
}
