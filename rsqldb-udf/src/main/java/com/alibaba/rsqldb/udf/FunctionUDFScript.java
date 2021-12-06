package com.alibaba.rsqldb.udf;

import java.io.File;
import org.apache.flink.api.common.accumulators.DoubleCounter;
import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.rocketmq.streams.script.service.udf.UDFScript;

public class FunctionUDFScript extends UDFScript {

    protected transient FunctionContext functionContext = new FunctionContext() {

        @Override
        public MetricGroup getMetricGroup() {
            return null;
        }

        @Override
        public File getCachedFile(String name) {
            return null;
        }

        @Override
        public int getNumberOfParallelSubtasks() {
            return 0;
        }

        @Override
        public int getIndexOfThisSubtask() {
            return 0;
        }

        @Override
        public IntCounter getIntCounter(String name) {
            return null;
        }

        @Override
        public LongCounter getLongCounter(String name) {
            return null;
        }

        @Override
        public DoubleCounter getDoubleCounter(String name) {
            return null;
        }

        @Override
        public Histogram getHistogram(String name) {
            return null;
        }

        @Override
        public String getJobParameter(String key, String defaultValue) {
            return "0";
        }
    };

    public FunctionUDFScript(String methodName, String functionName) {
        this.methodName = methodName;
        this.functionName = functionName;
//        this.initMethodName = "open";
        initParameters = new Object[] {functionContext};
    }

    public FunctionUDFScript() {

    }
}
