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
package com.alibaba.rsqldb.clients;

import org.apache.rocketmq.streams.common.compiler.CustomJavaCompiler;
import org.apache.rocketmq.streams.common.topology.graph.JobGraph;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.script.ScriptComponent;
import org.junit.Test;

public class CustomJavaCompilerTest {

    @Test
    public void testJavaCompilerTest() {
        String sourceCode = "package com.aliyun.et.industry.stream.domain.dipper.udaf;\n" +
            "\n" +
            "import org.apache.flink.table.functions.AggregateFunction;\n" +
            "import org.apache.rocketmq.streams.common.utils.CollectionUtil;\n" +
            "\n" +
            "/**\n" +
            " * @author fgm\n" +
            " * @date 2023/5/10\n" +
            " * @description 均值计算函数  average(`value`,scale) value: 当前数据 scale: 精度值,可为空，默认为2\n" +
            " */\n" +
            "\n" +
            "public class AverageAccumulator extends AggregateFunction<Number, AverageAccumulator.AverageAccum> {\n" +
            "\n" +
            "    @Override\n" +
            "    public AverageAccum createAccumulator() {\n" +
            "        return new AverageAccum();\n" +
            "    }\n" +
            "\n" +
            "    @Override\n" +
            "    public Number getValue(AverageAccum averageAccum) {\n" +
            "        return averageAccum._avg;\n" +
            "    }\n" +
            "\n" +
            "    /**\n" +
            "     * 计算平均值\n" +
            "     *\n" +
            "     * @param accumulator\n" +
            "     * @param parameters\n" +
            "     */\n" +
            "    public void accumulate(AverageAccumulator.AverageAccum accumulator, Object... parameters) {\n" +
            "        if (CollectionUtil.isEmpty(parameters)) {\n" +
            "            //log.info(\"AverageAccumulator parameters is empty!\");\n" +
            "            return;\n" +
            "        }\n" +
            "        if (parameters.length < 1) {\n" +
            "            //log.info(\"AverageAccumulator parameters.length less 1\");\n" +
            "            return;\n" +
            "        }\n" +
            "        Double value = ParameterUtil.getDouble(parameters[0]);\n" +
            "        if (null == value) {\n" +
            "            //log.info(\"AverageAccumulator value is null\");\n" +
            "            return;\n" +
            "        }\n" +
            "        int scale = getScale(parameters);\n" +
            "        accumulator._count++;\n" +
            "        accumulator._sum = accumulator._sum + value;\n" +
            "        double average = accumulator._sum / accumulator._count;\n" +
            "        accumulator._avg = NumberUtil.getDoubleValue(average, scale);\n" +
            "    }\n" +
            "\n" +
            "    /**\n" +
            "     * 获取精度\n" +
            "     *\n" +
            "     * @param parameters\n" +
            "     * @return\n" +
            "     */\n" +
            "    private int getScale(Object[] parameters) {\n" +
            "        //默认\n" +
            "        if (parameters.length < 2) {\n" +
            "            return 2;\n" +
            "        }\n" +
            "        Integer scale = ParameterUtil.getInteger(parameters[1]);\n" +
            "        return scale;\n" +
            "    }\n" +
            "\n" +
            "    public static class AverageAccum {\n" +
            "        private double _count;\n" +
            "        private double _sum;\n" +
            "        private double _avg;\n" +
            "    }\n" +
            "\n" +
            "}\n";

        CustomJavaCompiler compiler = new CustomJavaCompiler(sourceCode);
        Class o = compiler.compileClass();
        Object object = ReflectUtil.forInstance(o);
        ScriptComponent scriptComponent = ScriptComponent.getInstance();
        System.out.println(object.getClass());

        if (object instanceof org.apache.flink.table.functions.AggregateFunction) {
            System.out.println(object.getClass());
        }
    }

    @Test
    public void testUDF() throws InterruptedException {
        JobGraph jobGraph = SQLExecutionEnvironment.getExecutionEnvironment().createJobFromPath("TEST", "classpath://function.sql");
        jobGraph.start();

        Thread.sleep(10000000);
    }

}


