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
package com.alibaba.rsqldb.parser.udf;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.DoubleCounter;
import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.externalresource.ExternalResourceInfo;
import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.metrics.groups.OperatorMetricGroup;

public class EmptyRuntimeContext implements RuntimeContext {

    @Override
    public JobID getJobId() {
        return null;
    }

    @Override
    public String getTaskName() {
        return null;
    }

    @Override
    public OperatorMetricGroup getMetricGroup() {
        return null;
    }

    @Override
    public int getNumberOfParallelSubtasks() {
        return 0;
    }

    @Override
    public int getMaxNumberOfParallelSubtasks() {
        return 0;
    }

    @Override
    public int getIndexOfThisSubtask() {
        return 0;
    }

    @Override
    public int getAttemptNumber() {
        return 0;
    }

    @Override
    public String getTaskNameWithSubtasks() {
        return null;
    }

    @Override
    public ExecutionConfig getExecutionConfig() {
        return null;
    }

    @Override
    public ClassLoader getUserCodeClassLoader() {
        return null;
    }

    @Override
    public void registerUserCodeClassLoaderReleaseHookIfAbsent(String s, Runnable runnable) {

    }

    @Override
    public <V, A extends Serializable> void addAccumulator(String s, Accumulator<V, A> accumulator) {

    }

    @Override
    public <V, A extends Serializable> Accumulator<V, A> getAccumulator(String s) {
        return null;
    }

    @Override
    public IntCounter getIntCounter(String s) {
        return null;
    }

    @Override
    public LongCounter getLongCounter(String s) {
        return null;
    }

    @Override
    public DoubleCounter getDoubleCounter(String s) {
        return null;
    }

    @Override
    public Histogram getHistogram(String s) {
        return null;
    }

    @Override
    public Set<ExternalResourceInfo> getExternalResourceInfos(String s) {
        return null;
    }

    @Override
    public boolean hasBroadcastVariable(String s) {
        return false;
    }

    @Override
    public <RT> List<RT> getBroadcastVariable(String s) {
        return null;
    }

    @Override
    public <T, C> C getBroadcastVariableWithInitializer(String s, BroadcastVariableInitializer<T, C> initializer) {
        return null;
    }

    @Override
    public DistributedCache getDistributedCache() {
        return null;
    }

    @Override
    public <T> ValueState<T> getState(ValueStateDescriptor<T> descriptor) {
        return null;
    }

    @Override
    public <T> ListState<T> getListState(ListStateDescriptor<T> descriptor) {
        return null;
    }

    @Override
    public <T> ReducingState<T> getReducingState(ReducingStateDescriptor<T> descriptor) {
        return null;
    }

    @Override
    public <IN, ACC, OUT> AggregatingState<IN, OUT> getAggregatingState(
        AggregatingStateDescriptor<IN, ACC, OUT> descriptor) {
        return null;
    }

    @Override
    public <UK, UV> MapState<UK, UV> getMapState(MapStateDescriptor<UK, UV> descriptor) {
        return null;
    }
}
