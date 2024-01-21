package com.alibaba.rsqldb.parser.function;

import com.alibaba.rsqldb.parser.builder.SelectSqlBuilder;
import com.alibaba.rsqldb.parser.builder.WindowBuilder;
import com.alibaba.rsqldb.parser.result.IParseResult;
import com.alibaba.rsqldb.parser.result.VarParseResult;
import com.alibaba.rsqldb.parser.sqlnode.AbstractSelectNodeParser;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.rocketmq.streams.window.operator.AbstractWindow;

/**
 * session window parser
 *
 * @author arthur
 */
public class SessionParser extends AbstractSelectNodeParser<SqlBasicCall> {

    /**
     * 根据单位转化值为秒的单位
     *
     * @param interval
     * @param timeUnit
     * @return
     */
    protected static int convert2Second(int interval, TimeUnit timeUnit) {
        int timeout = interval;
        if (TimeUnit.SECOND == timeUnit) {
            return interval;
        }
        if (timeUnit != null) {
            if (TimeUnit.MINUTE == timeUnit) {
                timeout = interval * 60;
            } else if (TimeUnit.HOUR == timeUnit) {
                timeout = interval * 60 * 60;
            } else if (TimeUnit.DAY == timeUnit) {
                timeout = interval * 24 * 60 * 60;
            } else {
                throw new RuntimeException("can not this time unit :" + timeUnit.toString()
                    + ", support second,minute,hour,day only!");
            }
        }
        return timeout;
    }

    @Override
    public IParseResult parse(SelectSqlBuilder builder, SqlBasicCall sqlBasicCall) {
        SqlNode[] operands = sqlBasicCall.getOperands();
        SqlIntervalLiteral sqlIntervalLiteral = (SqlIntervalLiteral)operands[1];
        WindowBuilder windowBuilder = new WindowBuilder();
        windowBuilder.setType(AbstractWindow.SESSION_WINDOW);
        windowBuilder.setOwner(builder);
        setWindowParameter(windowBuilder, sqlIntervalLiteral);
        windowBuilder.setTimeFieldName(operands[0].toString());
        builder.setWindowBuilder(windowBuilder);
        return new VarParseResult(null);
    }

    private void setWindowParameter(WindowBuilder builder, SqlIntervalLiteral intervalLiteral) {
        SqlIntervalLiteral.IntervalValue intervalValue = (SqlIntervalLiteral.IntervalValue)intervalLiteral.getValue();
        TimeUnit unit = intervalValue.getIntervalQualifier().getUnit();
        //        builder.setLocalStorageOnly(TimeUnit.SECOND == unit);
        int interval = -1;
        try {
            interval = Integer.valueOf(intervalValue.getIntervalLiteral());
            interval = convert2Second(interval, unit);
        } catch (Exception e) {
            throw new RuntimeException("can not parser interval value !" + intervalValue.getIntervalLiteral(), e);
        }
        builder.setTimeout(interval);
    }

    @Override
    public boolean support(Object sqlNode) {
        if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall)sqlNode;
            if ("session".equalsIgnoreCase(sqlBasicCall.getOperator().getName())) {
                return true;
            }
        }
        return false;
    }
}