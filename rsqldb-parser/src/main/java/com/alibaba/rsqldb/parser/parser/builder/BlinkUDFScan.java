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

import com.alibaba.rsqldb.dim.intelligence.AbstractIntelligenceCache;
import com.alibaba.rsqldb.udf.FunctionUDFScript;
import com.alibaba.rsqldb.udf.udaf.BlinkUDAFScript;
import com.alibaba.rsqldb.udf.udf.BlinkUDFScript;
import com.alibaba.rsqldb.udf.udtf.BlinkUDTFScript;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.rocketmq.streams.common.calssscaner.AbstractScan;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.utils.FileUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.service.udf.UDFScript;
import sun.misc.JarFilter;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 支持blink udf的扫描，指定扫描路径完成udf函数扫描，会把jar包中所有udf扫描出来进行处处，目标把blink udf转化成dipper函数
 */
public class BlinkUDFScan extends AbstractScan {
    private static final Log logger = LogFactory.getLog(AbstractIntelligenceCache.class);

    private static BlinkUDFScan blinkUDFScan = new BlinkUDFScan();

    protected static final String CUSTOM_FUNCTION_JAR_PATH = ComponentCreator.CUSTOM_FUNCTION_JAR_PATH;

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
    protected transient Map<String, String> extendsDirFoUDF = new HashMap<>();//packageName,dir;classNAME;METHOD,dir

    static {
        notSupportUDF.add("AegisBinForLag");
        notSupportUDF.add("UDFIPRegion");
    }

    private BlinkUDFScan() {
    }

    public static BlinkUDFScan getInstance() {
        return blinkUDFScan;
    }

    /**
     * 阿里内部使用
     *
     * @param jarFilePath
     */
    @Deprecated
    public void scan(String jarFilePath, String classname, String functionName) {
        scanInnerBlinkUDF();
        String localJarFielPath = ComponentCreator.getProperties().getProperty(CUSTOM_FUNCTION_JAR_PATH);
        if (StringUtil.isEmpty(localJarFielPath)) {
            String homeDir = System.getProperty("home.dir");
            if (StringUtil.isEmpty(homeDir)) {
                throw new IllegalArgumentException("home.dir is null");
            }
            localJarFielPath = homeDir + "/custom";
        }
        scanFromLocalFile(localJarFielPath, jarFilePath, functionName);
    }

    /**
     * 扫描rocketmq内部自带blink udf函数
     */
    public void scanInnerBlinkUDF() {
        if (hasScane.compareAndSet(false, true)) {
            scan(null, "com.aliyun.sec.dw.blink.udf");
            scan(null, "com.aliyun.sec.dw.blink.udf");
            scan(null, "com.aliyun.sec.dw.rt.udf");
            scan(null, "com.aliyun.sec.dw.udf");
            scan(null, "com.aliyun.sec.lyra");
            scan(null, "com.aliyun.sec.secdata");
            scan(null, "com.lyra");
            scan(null, "com.sas.zing.blink.udf");
            scan(null, "com.self");
            scan(null, "com.aliyun.sec.sas");
            scan(null, "com.aliyun.yundun.dipper.sql.udf");
            scan(null, "com.aliyun.isec.seraph.udtf");
            scan(null, "com.lyra.udf.ext");
            scan(null, "org.apache.rocketmq.streams.script.function.impl.flatmap");
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
                throw new RuntimeException("can not parse url for udf jar " + jarDir, e);
            }
            URLClassLoader urlClassLoader = new URLClassLoader(new URL[] {url});
            this.scanClassDir(file, packageName, urlClassLoader, null);

        } else {
            this.scanPackage(packageName);
        }
    }

    /**
     * 扫描本地目录下用户自定义的udf
     *
     * @param jarPath      如果为null，在类路径扫描
     * @param packageName
     */
    public void scanFromLocalFile(String jarPath, String packageName, String functionName) {
        String jarDir = decodeUrl(jarPath);
        if (StringUtil.isNotEmpty(jarDir)) {
            List<File> jars = new ArrayList<>();
            File file = new File(jarDir);
            if (file.isDirectory()) {
                logger.info("BlinkUDFScan file is: " + file.getAbsolutePath());
                File[] files = file.listFiles(new JarFilter());
                jars.addAll(Arrays.asList(files));
            } else if (file.getName().endsWith(".jar")) {
                jars.add(file);
            }
            for (File jar : jars) {
                URL url = null;
                try {
                    jarDir = "file:" + jar.getCanonicalPath();
                    url = new URL(jarDir);
                    URLClassLoader urlClassLoader = new URLClassLoader(new URL[] {url});
                    this.extendsDirFoUDF.put(packageName, jar.getCanonicalPath());
                    this.scanClassDir(jar, packageName, urlClassLoader, functionName);
                } catch (MalformedURLException e) {
                    throw new RuntimeException("can not parse url for udf jar " + jarDir, e);
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }

        } else {
            this.scanPackage(packageName);
        }
    }

    /**
     * 扫描某个目录下jar包的包名
     *
     * @param jarDir      如果为null，在类路径扫描
     * @param packageName
     */
    public void scanFromUrl(String jarDir, String packageName, String functionName) {
        if (StringUtil.isNotEmpty(jarDir)) {
            File file = new File(jarDir);
            URL url = null;
            try {
                jarDir = "file:" + jarDir;
                url = new URL(jarDir);
            } catch (MalformedURLException e) {
                throw new RuntimeException("can not parse url for udf jar " + jarDir, e);
            }
            URLClassLoader urlClassLoader = new URLClassLoader(new URL[] {url}, this.getClass().getClassLoader());
            this.scanClassDir(file, packageName, urlClassLoader, functionName);

        } else {
            this.scanPackage(packageName);
        }
    }

    public void registerBlinkUDF(String dir, String packageName) {
        this.extendsDirFoUDF.put(packageName, dir);
        this.scanJarsFromDir(dir, packageName);
    }

    public void registerJarUDF(String dir, String className, String methodName) {
        this.extendsDirFoUDF.put(MapKeyUtil.createKey(className, methodName), dir);
        this.scanJarsFromDir(dir, ReflectUtil.forClass(className).getPackage().getName());
    }

    @Override
    protected void doProcessor(Class clazz, String functionName) {
        try {
            String blinkSuperClassName = getBlinkSuperClassName(clazz);
            if (!"".equalsIgnoreCase(blinkSuperClassName)) {
                if (notSupportUDF.contains(clazz.getSimpleName())) {
                    return;
                }
                UDFScript script = null;
                if (TableFunction.class.isAssignableFrom(clazz) || TableFunction.class.getSimpleName().equalsIgnoreCase(blinkSuperClassName)) {
                    script = new BlinkUDTFScript();
                } else if (AggregateFunction.class.isAssignableFrom(clazz) || AggregateFunction.class.getSimpleName().equalsIgnoreCase(blinkSuperClassName)) {
                    script = new BlinkUDAFScript();
                } else {
                    script = new BlinkUDFScript();
                }
                script.setFullClassName(clazz.getName());
                script.setFunctionName(clazz.getSimpleName());
                className2Scripts.put(clazz.getName(), script);
                //                String dir=this.extendsDirFoUDF.get(clazz.getPackage().getName());
                String dir = this.extendsDirFoUDF.get(clazz.getName());
                if (dir != null) {
                    script.setValue(FileUtil.LOCAL_FILE_HEADER + dir);
                }
            } else if (clazz.isAnnotationPresent(Function.class)) {
                registerAnnotationFunction(clazz);
            } else {
                registerUserFunction(clazz, functionName);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private String getBlinkSuperClassName(Class clazz) {
        if (Object.class.getSimpleName().equalsIgnoreCase(clazz.getSimpleName())) {
            return "";
        }
        if (ScalarFunction.class.getSimpleName().equalsIgnoreCase(clazz.getSimpleName()) ||
            TableFunction.class.getSimpleName().equalsIgnoreCase(clazz.getSimpleName()) ||
            AggregateFunction.class.getSimpleName().equalsIgnoreCase(clazz.getSimpleName())) {
            return clazz.getSimpleName();
        } else {
            return this.getBlinkSuperClassName(clazz.getSuperclass());
        }
    }

    /**
     * 将带有@FunctionMethod注解的方法注册为udfscript
     *
     * @param clazz
     */
    public void registerAnnotationFunction(Class clazz) {
        List<Method> methods = getMethodList(clazz);

        for (Method method : methods) {
            FunctionMethod annotation = method.getAnnotation(FunctionMethod.class);
            String functionName = annotation.value();
            if (functionName != null && !"".equalsIgnoreCase(functionName)) {
                registerFunctionUDFScript(clazz, method.getName(), functionName);
            }
            registerFunctionUDFScript(clazz, method.getName(), method.getName());
            if (StringUtil.isNotEmpty(annotation.alias())) {
                String aliases = annotation.alias();
                if (aliases.indexOf(",") != -1) {
                    String[] values = aliases.split(",");
                    for (String alias : values) {
                        registerFunctionUDFScript(clazz, method.getName(), alias);
                    }
                } else {
                    registerFunctionUDFScript(clazz, method.getName(), aliases);
                }
            }
        }
    }

    /**
     * 根据class和函数名字注册udf，如果方法列表中包含eval方法，则将functionname全部与eval方法绑定并注册UDFScript 如果class中不包含eval方法，则将functionname与对应的方法名进行绑定并注册
     *
     * @param clazz
     * @param functionName
     */
    public void registerUserFunction(Class clazz, String functionName) {
        List<Method> evalMethods = getEvalMethodList(clazz);
        if (evalMethods != null && evalMethods.size() > 0) {
            registerFunctionUDFScript(clazz, "eval", functionName);
        } else {
            Method[] methods = clazz.getMethods();
            if (methods != null && methods.length > 0) {
                for (Method method : methods) {
                    if (functionName != null && functionName.equalsIgnoreCase(method.getName())) {
                        registerFunctionUDFScript(clazz, method.getName(), functionName);
                    }
                }
            }
        }

    }

    public void registerFunctionUDFScript(Class clazz, String methodName, String functionName) {
        UDFScript script = new FunctionUDFScript(methodName, functionName);
        script.setFullClassName(clazz.getName());
        //        script.setFunctionName(clazz.getSimpleName());
        String dir = this.extendsDirFoUDF.get(clazz.getName());
        if (dir != null) {
            script.setValue(FileUtil.LOCAL_FILE_HEADER + dir);
        }
        className2Scripts.put(createName(clazz.getName(), functionName), script);

    }

    /**
     * 获取所有带FunctionMethod标注的方法
     *
     * @param clazz
     * @return
     */
    private List<Method> getMethodList(Class clazz) {
        Method[] methods = clazz.getMethods();
        List<Method> methodList = new ArrayList<Method>();
        for (Method method : methods) {
            if (method.getAnnotation(FunctionMethod.class) != null) {
                methodList.add(method);
            }
        }
        return methodList;
    }

    /**
     * 提取class中方法名为eval的函数列表
     *
     * @param clazz
     * @return
     */
    private List<Method> getEvalMethodList(Class clazz) {
        Method[] methods = clazz.getMethods();
        List<Method> methodList = new ArrayList<Method>();
        for (Method method : methods) {
            if ("eval".equalsIgnoreCase(method.getName())) {
                methodList.add(method);
            }
        }
        return methodList;
    }

    public UDFScript getScript(String className, String functionName) {
        UDFScript udfScript = className2Scripts.get(className);
        if (udfScript == null) {
            udfScript = className2Scripts.get(createName(className, functionName));
        }
        return udfScript;
    }

    public String createName(String... names) {
        StringBuilder builder = new StringBuilder();
        if (names != null && names.length > 0) {
            for (String name : names) {
                builder.append(name).append('-');
            }
            return builder.substring(0, builder.length() - 1);
        }
        return "";
    }
}
