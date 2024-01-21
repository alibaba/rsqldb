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
package com.alibaba.rsqldb.parser.sql.stage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.rocketmq.streams.common.batchsystem.BatchFinishMessage;
import org.apache.rocketmq.streams.common.channel.impl.view.ViewSink;
import org.apache.rocketmq.streams.common.channel.sink.ISink;
import org.apache.rocketmq.streams.common.channel.source.AbstractSource;
import org.apache.rocketmq.streams.common.channel.source.systemmsg.NewSplitMessage;
import org.apache.rocketmq.streams.common.channel.source.systemmsg.RemoveSplitMessage;
import org.apache.rocketmq.streams.common.checkpoint.CheckPointMessage;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.optimization.IHomologousCalculate;
import org.apache.rocketmq.streams.common.optimization.IHomologousOptimization;
import org.apache.rocketmq.streams.common.threadpool.ScheduleFactory;
import org.apache.rocketmq.streams.common.threadpool.ThreadPoolFactory;
import org.apache.rocketmq.streams.common.topology.graph.JobGraph;
import org.apache.rocketmq.streams.common.topology.model.ChainPipeline;
import org.apache.rocketmq.streams.common.topology.stages.OutputChainStage;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MergeSQLJobStage<T extends IMessage> extends OutputChainStage<T> implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(MergeSQLJobStage.class);
    /**
     * 总处理数据数
     */
    private final transient AtomicLong COUNT = new AtomicLong(0);
    /**
     * 触发规则的个数
     */
    private final transient AtomicLong FIRE_RULE_COUNT = new AtomicLong(0);
    /**
     * 动态加载的pipeline，pipeline的source type是view，且tablename=sink的view name
     */
    protected transient volatile Map<String, ChainPipeline<?>> subPipelines = new HashMap<>();
    protected int parallelTasks = 4;
    /**
     * Supports mul-threaded task execution
     */
    protected transient ExecutorService executorService;
    /**
     * 最早的处理时间
     */
    protected transient volatile Long firstReceiveTime = null;

    /**
     * Automatically parses pipelines, generates pre-filter fingerprints and expression estimates
     */
    protected transient IHomologousOptimization homologousOptimization;
    protected int addHomologousOptimizationPipelineCount = 0;
    protected IHomologousCalculate homologousCalculate;

    @Override
    protected IMessage handleMessage(IMessage message, AbstractContext context) {
        List<ChainPipeline<?>> currentPipelines = new ArrayList();
        synchronized (this) {
            currentPipelines.addAll(subPipelines.values());
        }

        if (CollectionUtil.isEmpty(currentPipelines)) {
            return null;
        }
        //Calculate QPS
        double qps = calculateQPS();
        if (COUNT.get() % 10000 == 0) {
            LOGGER.debug("[{}] qps is {}, the count is {}", getName(), qps, COUNT.get());
        }

        if (this.homologousCalculate != null) {
            homologousCalculate.calculate(message, context);
        }
        boolean onlyOne = currentPipelines.size() == 1;

        CountDownLatch countDownLatch = null;

        if (currentPipelines.size() > 1 && parallelTasks > 1 && executorService != null) {
            countDownLatch = new CountDownLatch(currentPipelines.size());
        }
        int index = 0;
        for (ChainPipeline<?> pipeline : currentPipelines) {

            IMessage copyMessage = message;
            if (!onlyOne) {
                copyMessage = message.deepCopy();
            }
            AbstractContext newContext = context.copy();
            HomologousTask homologousTask = new HomologousTask((Message)copyMessage, newContext, pipeline, index);
            if (executorService != null && countDownLatch != null) {
                homologousTask.setCountDownLatch(countDownLatch);
                executorService.execute(homologousTask);
            } else {
                homologousTask.run();
            }
            index++;
        }

        if (countDownLatch != null) {
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        return null;
    }

    @Override
    public boolean isAsyncNode() {
        return false;
    }

    @Override
    public synchronized void checkpoint(IMessage message, AbstractContext context,
        CheckPointMessage checkPointMessage) {
        Collection<ChainPipeline<?>> currentPipelines = subPipelines.values();
        sendSystem(message, context, currentPipelines);
    }

    @Override
    public synchronized void addNewSplit(IMessage message, AbstractContext context, NewSplitMessage newSplitMessage) {
        Collection<ChainPipeline<?>> currentPipelines = subPipelines.values();
        sendSystem(message, context, currentPipelines);
    }

    @Override
    public synchronized void removeSplit(IMessage message, AbstractContext context,
        RemoveSplitMessage removeSplitMessage) {
        Collection<ChainPipeline<?>> currentPipelines = subPipelines.values();
        sendSystem(message, context, currentPipelines);
    }

    @Override
    public synchronized void batchMessageFinish(IMessage message, AbstractContext context,
        BatchFinishMessage checkPointMessage) {
        Collection<ChainPipeline<?>> currentPipelines = subPipelines.values();
        sendSystem(message, context, currentPipelines);
    }

    @Override
    public void setSink(ISink<?> channel) {
        ViewSink viewSink = (ViewSink)channel;
        this.sink = channel;
        this.setNameSpace(channel.getNameSpace());
        this.setLabel(channel.getName());
        this.setName(viewSink.getViewTableName());
        this.setParallelTasks(viewSink.getParallelTasks());
    }

    protected String getOrCreateFingerNameSpace() {
        return getName();
    }

    /**
     * Print QPS in the scene without fingerprint on
     */
    protected double calculateQPS() {
        if (firstReceiveTime == null) {
            synchronized (this) {
                if (firstReceiveTime == null) {
                    firstReceiveTime = System.currentTimeMillis();
                }
            }
        }
        long second = ((System.currentTimeMillis() - firstReceiveTime) / 1000);
        if (second == 0) {
            second = 1;
        }
        return (double)(COUNT.incrementAndGet() / second);
    }

    @Override
    public synchronized void startJob() {
        if (this.homologousOptimization == null) {
            this.addHomologousOptimizationPipelineCount = this.subPipelines.size();
            Iterable<IHomologousOptimization> iterable = ServiceLoader.load(IHomologousOptimization.class);
            Iterator<IHomologousOptimization> it = iterable.iterator();
            if (it.hasNext()) {
                IHomologousOptimization homologousOptimization = it.next();
                this.homologousOptimization = homologousOptimization;
                this.run();
            }
        }
        ScheduleFactory.getInstance().execute(getNameSpace() + "-" + getName() + "-merge_sql_schedule", this, 60, 60,
            TimeUnit.SECONDS);

        if (this.parallelTasks > 1) {
            executorService = ThreadPoolFactory.createFixedThreadPool(this.parallelTasks, "mergesql");
        }
    }

    @Override
    public void run() {
        if (this.subPipelines.size() != this.addHomologousOptimizationPipelineCount) {
            synchronized (this) {
                if (this.subPipelines.size() != this.addHomologousOptimizationPipelineCount) {
                    homologousOptimization.optimizate(new ArrayList<>(this.subPipelines.values()));
                    this.homologousCalculate = homologousOptimization.createHomologousCalculate();
                    this.addHomologousOptimizationPipelineCount = subPipelines.size();
                }
            }

        }
    }

    public synchronized void setSubJobGraphs(Collection<JobGraph> jobGraphs) {
        if (jobGraphs != null) {

            Map<String, ChainPipeline<?>> newJobGraphMap = new HashMap<>();
            for (JobGraph jobGraph : jobGraphs) {
                if (jobGraph.getPipelines().size() != 1) {
                    throw new RuntimeException(
                        "MergeSQLExecutionEnvironment can not support double flow， please user "
                            + "SQLExecutionEnvironment submit sql");
                }
                ChainPipeline chainPipeline = jobGraph.getPipelines().get(0);
                newJobGraphMap.put(chainPipeline.getName(), chainPipeline);
            }
            this.subPipelines = newJobGraphMap;
        }
    }

    @Override
    public void stopJob() {
        super.stopJob();
        ScheduleFactory.getInstance().cancel(getNameSpace() + "-" + getName() + "-merge_sql_schedule");

        if (executorService != null) {
            executorService.shutdown();
            executorService = null;
        }
        for (ChainPipeline<?> chainPipeline : this.subPipelines.values()) {
            chainPipeline.destroy();
        }
        this.subPipelines = new HashMap<>();
    }

    public void setParallelTasks(int parallelTasks) {
        this.parallelTasks = parallelTasks;
    }

    class HomologousTask implements Runnable {
        protected Message message;
        protected AbstractContext context;
        protected ChainPipeline<?> pipeline;
        protected int index;
        protected CountDownLatch countDownLatch;

        public HomologousTask(Message message, AbstractContext context, ChainPipeline<?> pipeline, int index) {
            this.message = message;
            this.context = context;
            this.pipeline = pipeline;
            this.index = index;
        }

        @Override
        public void run() {
            try {
                AbstractSource source = (AbstractSource)pipeline.getSource();
                source.executeMessage(message, context);
            } catch (Exception e) {
                LOGGER.error("[{}] pipeline execute error {}", getName(), pipeline.getName(), e);
            } finally {
                if (this.countDownLatch != null) {
                    this.countDownLatch.countDown();
                }
            }
        }

        public CountDownLatch getCountDownLatch() {
            return countDownLatch;
        }

        public void setCountDownLatch(CountDownLatch countDownLatch) {
            this.countDownLatch = countDownLatch;
        }
    }
}
