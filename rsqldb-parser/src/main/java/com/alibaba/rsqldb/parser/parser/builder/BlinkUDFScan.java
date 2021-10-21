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
package com.alibaba.rsqldb.parser.parser.builder;

import java.io.File;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.calssscaner.AbstractScan;
import org.apache.rocketmq.streams.common.utils.FileUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.service.udf.UDFScript;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import com.alibaba.rsqldb.udf.udaf.BlinkUDAFScript;
import com.alibaba.rsqldb.udf.udf.BlinkUDFScript;
import com.alibaba.rsqldb.udf.udtf.BlinkUDTFScript;

/**
 * 支持blink udf的扫描，指定扫描路径完成udf函数扫描，会把jar包中所有udf扫描出来进行处处，目标把blink udf转化成dipper函数
 */
public class BlinkUDFScan extends AbstractScan {
    private static BlinkUDFScan blinkUDFScan = new BlinkUDFScan();

    protected String BLINK_UDF_JAR_PATH = ComponentCreator.BLINK_UDF_JAR_PATH;

    /**
     * udf的class name和dipper blink script的对应关系
     */
    protected Map<String, UDFScript> className2Scripts = new HashMap<>();



    /**
     * 保证blink udf扫描可重入
     */
    protected AtomicBoolean hasScane = new AtomicBoolean(false);

    /**
     * dipper不能识别的udf，主要是黑名单作用
     */
    protected transient static Set<String> notSupportUDF = new HashSet<>();
    protected transient Map<String,String> extendsDirFoUDF=new HashMap<>();//packageName,dir;classNAME;METHOD,dir

    static {
        notSupportUDF.add("AegisBinForLag");
        notSupportUDF.add("UDFIPRegion");
    }

    private BlinkUDFScan() {}

    public static BlinkUDFScan getInstance() {
        return blinkUDFScan;
    }

    /**
     * 阿里内部使用
     *
     * @param jarFilePath
     */
    @Deprecated
    public void scan(String jarFilePath) {
        if (hasScane.compareAndSet(false, true)) {
            scan(jarFilePath, "com.aliyun.sec.dw.blink.udf");
            scan(jarFilePath, "com.aliyun.sec.dw.blink.udf");
            scan(jarFilePath, "com.aliyun.sec.dw.rt.udf");
            scan(jarFilePath, "com.aliyun.sec.dw.udf");
            scan(jarFilePath, "com.aliyun.sec.lyra");
            scan(jarFilePath, "com.aliyun.sec.secdata");
            scan(jarFilePath, "com.lyra");
            scan(jarFilePath, "com.sas.zing.blink.udf");
            scan(jarFilePath, "com.self");
            scan(jarFilePath, "com.aliyun.sec.sas");
            scan(jarFilePath, "com.aliyun.yundun.dipper.sql.udf");
            scan(jarFilePath, "com.aliyun.isec.seraph.udtf");
            scan(jarFilePath, "com.lyra.udf.ext");

        }

    }

    /**
     * 扫描某个目录下jar包的包名
     *
     * @param jarDir      如果为null，在类路径扫描
     * @param packageName
     */
    public void scan(String jarDir, String packageName) {
        if (StringUtil.isNotEmpty(jarDir)) {
            File file = new File(jarDir);
            URL url = null;
            try {
                jarDir = "file:" + jarDir;
                url = new URL(jarDir);
            } catch (MalformedURLException e) {
                throw new RuntimeException("can not parser url for udf jar " + jarDir, e);
            }
            URLClassLoader urlClassLoader = new URLClassLoader(new URL[] {url});
            this.scanClassDir(file, packageName, urlClassLoader);

        } else {
            this.scanPackage(packageName);
        }
    }

    public void registBlinkUDF(String dir,String packageName){
        this.extendsDirFoUDF.put(packageName,dir);
        this.scanJarsFromDir(dir,packageName);
    }

    public void registJarUDF(String dir,String className,String methodName){
        this.extendsDirFoUDF.put(MapKeyUtil.createKey(className,methodName),dir);
        this.scanJarsFromDir(dir,ReflectUtil.forClass(className).getPackage().getName());
    }

    @Override
    protected void doProcessor(Class clazz) {
        try {
            if (ScalarFunction.class.isAssignableFrom(clazz) || TableFunction.class.isAssignableFrom(clazz) || AggregateFunction.class.isAssignableFrom(clazz)) {
                if (notSupportUDF.contains(clazz.getSimpleName())) {
                    return;
                }
                UDFScript script = null;
                if (TableFunction.class.isAssignableFrom(clazz)) {
                    script = new BlinkUDTFScript();
                } else if (AggregateFunction.class.isAssignableFrom(clazz)) {
                    script = new BlinkUDAFScript();
                } else {
                    script = new BlinkUDFScript();
                }
                script.setFullClassName(clazz.getName());
                script.setFunctionName(clazz.getSimpleName());
                className2Scripts.put(clazz.getName(), script);
                String dir=this.extendsDirFoUDF.get(clazz.getPackage().getName());
                if(dir!=null){
                    script.setValue(FileUtil.LOCAL_FILE_HEADER+dir);
                }
            }
            Method[] methods=clazz.getMethods();
            for(Method method:methods){
                String dir=this.extendsDirFoUDF.get(MapKeyUtil.createKey(clazz.getName(),method.getName()));
                if(dir!=null){
                    UDFScript script = new UDFScript();
                    script.setFullClassName(clazz.getName());
                    script.setFunctionName(clazz.getSimpleName());
                    className2Scripts.put(clazz.getName(), script);
                    script.setValue(dir);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public UDFScript getScript(String className) {
        return className2Scripts.get(className);
    }
}
