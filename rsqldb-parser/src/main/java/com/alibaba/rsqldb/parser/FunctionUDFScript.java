package com.alibaba.rsqldb.parser;

import org.apache.rocketmq.streams.script.service.udf.UDFScript;

public class FunctionUDFScript extends UDFScript {

    public FunctionUDFScript(String methodName, String functionName) {
        this.methodName = methodName;
        this.functionName = functionName;
        this.initMethodName = "open";
        initParameters = new Object[0];
    }

    public FunctionUDFScript() {

    }
}
