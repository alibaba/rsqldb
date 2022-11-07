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

import com.alibaba.rsqldb.parser.parser.FunctionUDFScript;
import com.aliyun.oss.OSSClient;
import com.beust.jcommander.internal.Lists;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.rocketmq.streams.common.calssscaner.AbstractScan;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.utils.FileUtil;
import org.apache.rocketmq.streams.common.utils.IdUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.service.IUDFScan;
import org.apache.rocketmq.streams.script.service.udf.UDFScript;
import org.apache.rocketmq.streams.serviceloader.ServiceLoaderComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.JarFilter;

/**
 * 支持blink udf的扫描，指定扫描路径完成udf函数扫描，会把jar包中所有udf扫描出来进行处处，目标把blink udf转化成dipper函数
 *
 * @author junjie.cheng
 */
public class BlinkUDFScan extends AbstractScan {

    private static final Logger LOGGER = LoggerFactory.getLogger(BlinkUDFScan.class);
    private static final BlinkUDFScan BLINK_UDF_SCAN = new BlinkUDFScan();

    /**
     * udf的class name和dipper blink script的对应关系
     */
    protected Map<String, UDFScript> className2Scripts = new HashMap<>();

    /**
     * 保证blink udf扫描可重入
     */
    protected AtomicBoolean hasScan = new AtomicBoolean(false);

    /**
     * dipper不能识别的udf，主要是黑名单作用
     */
    protected transient static Set<String> notSupportUDF = new HashSet<>();
    /**
     * packageName,dir;classNAME;METHOD,dir
     */
    protected transient Map<String, String> extendsDirFoUDF = new HashMap<>();

    static {
        notSupportUDF.add("AegisBinForLag");
        notSupportUDF.add("UDFIPRegion");
    }

    private BlinkUDFScan() {
    }

    public static BlinkUDFScan getInstance() {
        return BLINK_UDF_SCAN;
    }

    public static void main(String[] args) {
        System.out.println(BlinkUDFScan.class.getClassLoader().getResource("").getPath());
        BlinkUDFScan udf = BlinkUDFScan.getInstance();
        String outSideJarFielPath = ComponentCreator.getProperties().getProperty(ComponentCreator.UDF_JAR_PATH);
        //        udf.scan("com.self.HexStrEncode");
        //        udf.scanFromLocalFile(outSideJarFielPath,"com.self.HexStrEncode");
        //        udf.scanFromFile("./","");
        //        udf.scanFromUrl("https://softwaretk.oss-cn-beijing.aliyuncs.com/extudf_2.0.jar", "");
        System.out.println("end");
    }

    /**
     * 阿里内部使用
     *
     * @param className    包名
     * @param functionName 方法名
     */
    public void scanClass(String className, String functionName) {
        scanInnerBlinkUDF();
        String filePath = ComponentCreator.getProperties().getProperty(ComponentCreator.UDF_JAR_PATH);
        if (filePath == null || "".equalsIgnoreCase(filePath)) {
            filePath = "./udflib";
        }
        if (filePath.startsWith("http://") || filePath.startsWith("https://")) {
            scanFromUrl(filePath, className, functionName);
        } else if (filePath.startsWith("oss://")) {
            scanFromOss(filePath, className, functionName);
        } else {
            scanFromLocal(filePath, className, functionName);
        }
    }

    private void scanFromOss(String filePath, String className, String functionName) {
        String ossUrl = filePath.substring(6); //url以oss://开头
        String accessKeyId = ComponentCreator.getProperties().getProperty(ComponentCreator.UDF_JAR_OSS_ACCESS_ID);
        String accesskeySecurity = ComponentCreator.getProperties().getProperty(ComponentCreator.UDF_JAR_OSS_ACCESS_KEY);

        String[] ossInfo = ossUrl.split("/");
        String endPoint = ossInfo.length > 0 ? ossInfo[0] : "";
        String bucketName = ossInfo.length > 1 ? ossInfo[1] : "";
        List<String> objectNames = ossInfo.length > 2 ? Arrays.asList(ossInfo[2].split(",")) : Lists.newArrayList();

        OSSClient ossClient = new OSSClient(endPoint, accessKeyId, accesskeySecurity);
        URL[] urls = new URL[objectNames.size()];
        for (int i = 0; i < objectNames.size(); i++) {
            urls[i] = ossClient.generatePresignedUrl(bucketName, objectNames.get(i), DateUtils.addMinutes(new Date(), 30));
        }
        URLClassLoader urlClassLoader = new URLClassLoader(urls, Thread.currentThread().getContextClassLoader());
        this.scanClassInJar(className, urlClassLoader, functionName);
        UDFScript udfScript = className2Scripts.get(className);
        udfScript.setValue(filePath);
    }

    /**
     * 扫描rocketmq内部自带blink udf函数
     */
    public void scanInnerBlinkUDF() {
        if (hasScan.compareAndSet(false, true)) {
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
            URLClassLoader urlClassLoader = new URLClassLoader(new URL[] {url}, this.getClass().getClassLoader());
            this.scanClassDir(file, packageName, urlClassLoader, null);

        } else {
            this.scanPackage(packageName);
        }
    }

    /**
     * 扫描本地目录下用户自定义的udf
     *
     * @param jarDir    如果为null，在类路径扫描
     * @param className 包名
     */
    public void scanFromLocal(String jarDir, String className, String functionName) {
        if (StringUtil.isNotEmpty(jarDir)) {
            List<File> jars = new ArrayList<>();
            File file = new File(jarDir);
            if (file.isDirectory()) {
                LOGGER.info("[{}] BlinkUDFScan_File{{}}", IdUtil.instanceId(), file.getAbsolutePath());
                File[] files = file.listFiles(new JarFilter());
                if (files != null) {
                    jars.addAll(Arrays.asList(files));
                }
            } else if (file.getName().endsWith(".jar")) {
                jars.add(file);
            }
            for (File jar : jars) {
                URL url = null;
                try {
                    jarDir = "file:" + jar.getCanonicalPath();
                    url = new URL(jarDir);
                    URLClassLoader urlClassLoader = new URLClassLoader(new URL[] {url}, this.getClass().getClassLoader());
                    this.extendsDirFoUDF.put(className, jar.getCanonicalPath());
                    this.scanClassDir(jar, className, urlClassLoader, functionName);
                } catch (MalformedURLException e) {
                    throw new RuntimeException("can not parse url for udf jar " + jarDir, e);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        } else {
            this.scanPackage(className);
        }
    }

    /**
     * 扫描URL中的类
     *
     * @param uri       如果为null，在类路径扫描
     * @param className 类名
     */
    public void scanFromUrl(String uri, String className, String functionName) {
        if (StringUtil.isNotEmpty(uri)) {
            URL url = null;
            try {
                url = new URL(uri);
            } catch (MalformedURLException e) {
                throw new RuntimeException("can not parse url for udf jar " + uri, e);
            }
            URLClassLoader urlClassLoader = new URLClassLoader(new URL[] {url}, this.getClass().getClassLoader());
            this.scanClassInJar(className, urlClassLoader, functionName);
            UDFScript udfScript = className2Scripts.get(className);
            udfScript.setValue(uri);
        } else {
            this.scanPackage(className);
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

    protected transient ServiceLoaderComponent<IUDFScan> udfScanServiceLoader = (ServiceLoaderComponent<IUDFScan>) ServiceLoaderComponent.getInstance(IUDFScan.class);

    @Override protected void doProcessor(Class clazz, String functionName) {
        try {
            if (notSupportUDF.contains(clazz.getSimpleName())) {
                return;
            }
            UDFScript udfScript = null;
            List<IUDFScan> udfScans = udfScanServiceLoader.loadService();
            if (udfScans != null) {

                for (IUDFScan udfScan : udfScans) {
                    if (udfScan.isSupport(clazz)) {
                        udfScript = udfScan.create(clazz, functionName);
                        break;
                    }
                }
            }
            if (udfScript != null) {
                udfScript.setFullClassName(clazz.getName());
                udfScript.setFunctionName(clazz.getSimpleName());
                className2Scripts.put(clazz.getName(), udfScript);
                String dir = this.extendsDirFoUDF.get(clazz.getName());
                if (dir != null) {
                    udfScript.setValue(FileUtil.LOCAL_FILE_HEADER + dir);
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

    /**
     * 将带有@FunctionMethod注解的方法注册为udfscript
     *
     * @param clazz<> className
     */
    public void registerAnnotationFunction(Class<?> clazz) {
        List<Method> methods = getMethodList(clazz);

        for (Method method : methods) {
            FunctionMethod annotation = method.getAnnotation(FunctionMethod.class);
            String functionName = annotation.value();
            if (functionName != null && !"".equalsIgnoreCase(functionName)) {
                registerFunctionUdfScript(clazz, method.getName(), functionName);
            }
            registerFunctionUdfScript(clazz, method.getName(), method.getName());
            if (StringUtil.isNotEmpty(annotation.alias())) {
                String aliases = annotation.alias();
                if (aliases.contains(",")) {
                    String[] values = aliases.split(",");
                    for (String alias : values) {
                        registerFunctionUdfScript(clazz, method.getName(), alias);
                    }
                } else {
                    registerFunctionUdfScript(clazz, method.getName(), aliases);
                }
            }
        }
    }

    /**
     * 根据class和函数名字注册udf，如果方法列表中包含eval方法，则将functionname全部与eval方法绑定并注册UDFScript 如果class中不包含eval方法，则将functionname与对应的方法名进行绑定并注册
     *
     * @param clazz        类名
     * @param functionName 方法名
     */
    public void registerUserFunction(Class<?> clazz, String functionName) {
        List<Method> evalMethods = getEvalMethodList(clazz);
        if (evalMethods.size() > 0) {
            registerFunctionUdfScript(clazz, "eval", functionName);
        } else {
            Method[] methods = clazz.getMethods();
            if (methods.length > 0) {
                for (Method method : methods) {
                    if (functionName != null && functionName.equalsIgnoreCase(method.getName())) {
                        registerFunctionUdfScript(clazz, method.getName(), functionName);
                    }
                }
            }
        }

    }

    public void registerFunctionUdfScript(Class<?> clazz, String methodName, String functionName) {
        UDFScript script = new FunctionUDFScript(methodName, functionName);
        script.setFullClassName(clazz.getName());
        String dir = this.extendsDirFoUDF.get(clazz.getName());
        if (dir != null) {
            script.setValue(FileUtil.LOCAL_FILE_HEADER + dir);
        }
        className2Scripts.put(createName(clazz.getName(), functionName), script);

    }

    /**
     * 获取所有带FunctionMethod标注的方法
     *
     * @param clazz 类名
     * @return 方法列表
     */
    private List<Method> getMethodList(Class<?> clazz) {
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
     * @param clazz 类名
     * @return 方法列表
     */
    private List<Method> getEvalMethodList(Class<?> clazz) {
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
