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

import com.alibaba.rsqldb.parser.parser.builder.SelectSqlBuilder;
import com.alibaba.rsqldb.parser.parser.builder.WindowBuilder;
import com.alibaba.rsqldb.parser.parser.result.IParseResult;
import com.alibaba.rsqldb.parser.parser.result.VarParseResult;
import com.alibaba.rsqldb.parser.parser.sqlnode.AbstractSelectNodeParser;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlIntervalLiteral.IntervalValue;
import org.apache.calcite.sql.SqlNode;
import org.apache.rocketmq.streams.common.configure.ConfigureFileKey;
import org.apache.rocketmq.streams.common.topology.model.IWindow;
import org.apache.rocketmq.streams.window.operator.AbstractWindow;

public class TumbleParser extends AbstractSelectNodeParser<SqlBasicCall> {

    @Override
    public IParseResult parse(SelectSqlBuilder builder, SqlBasicCall sqlBasicCall) {
        SqlNode[] operands = sqlBasicCall.getOperands();
        IParseResult fieldName = parseSqlNode(builder, operands[0]);
        SqlIntervalLiteral sqlIntervalLiteral = (SqlIntervalLiteral) operands[1];
        WindowBuilder windowBuilder=createWindowBuilder(builder,sqlIntervalLiteral,fieldName.getReturnValue());
        return new VarParseResult(null);
    }


    public static com.alibaba.rsqldb.parser.parser.builder.WindowBuilder createWindowBuilder(SelectSqlBuilder builder) {
        /**
         * 如果只有group by，没有指定窗口，则通过配置获取默认窗口大小，如果未指定，默认1个小时
         */
        int inteval = org.apache.rocketmq.streams.window.builder.WindowBuilder.getIntValue(ConfigureFileKey.DIPPER_WINDOW_DEFAULT_INERVAL_SIZE, 60);
        int timeUnitAdjust = org.apache.rocketmq.streams.window.builder.WindowBuilder.getIntValue(ConfigureFileKey.DIPPER_WINDOW_DEFAULT_TIME_UNIT_ADJUST, 60);
        com.alibaba.rsqldb.parser.parser.builder.WindowBuilder
            windowBuilder = new com.alibaba.rsqldb.parser.parser.builder.WindowBuilder();
        windowBuilder.setType(IWindow.TUMBLE_WINDOW);
        windowBuilder.setOwner(builder);
        windowBuilder.setSize(inteval);
        windowBuilder.setSlide(inteval);
        windowBuilder.setTimeUnitAdjust(timeUnitAdjust);
        windowBuilder.setTimeFieldName("");
        builder.setWindowBuilder(windowBuilder);
        return windowBuilder;
    }
    public static WindowBuilder createWindowBuilder(SelectSqlBuilder builder,SqlIntervalLiteral intervalLiteral,String fieldName){
        WindowBuilder windowBuilder = new WindowBuilder();
        windowBuilder.setType(AbstractWindow.TUMBLE_WINDOW);
        windowBuilder.setOwner(builder);
        setWindowParameter(true, windowBuilder, intervalLiteral);
        windowBuilder.setTimeFieldName(fieldName);
        builder.setWindowBuilder(windowBuilder);
        return windowBuilder;
    }

    public static WindowBuilder createWindowBuilder(SelectSqlBuilder builder,SqlIntervalLiteral emitBefore,SqlIntervalLiteral intervalLiteral,String fieldName){
        WindowBuilder windowBuilder=createWindowBuilder(builder,intervalLiteral,fieldName);
        IntervalValue intervalValue = (IntervalValue) intervalLiteral.getValue();
        TimeUnit unit = intervalValue.getIntervalQualifier().getUnit();
        int interval = Integer.valueOf(intervalValue.getIntervalLiteral());
        int emitTime = convert2Second(interval, unit);
        windowBuilder.setEmitBefore((long)emitTime);
        return windowBuilder;
    }

    public static void setWindowParameter(boolean isSize, WindowBuilder builder, SqlIntervalLiteral intervalLiteral) {
        IntervalValue intervalValue = (IntervalValue) intervalLiteral.getValue();
        TimeUnit unit = intervalValue.getIntervalQualifier().getUnit();
        builder.setLocalStorageOnly(TimeUnit.SECOND == unit && isSize);
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
            int windowStep = convert2Second(interval, unit);
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
        return convert2Second(interval, unit);
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
    protected static int convert2Second(int interval, TimeUnit timeUnit) {
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
