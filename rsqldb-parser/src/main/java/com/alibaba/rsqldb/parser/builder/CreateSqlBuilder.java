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
package com.alibaba.rsqldb.parser.builder;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import com.alibaba.rsqldb.parser.SqlBuilderResult;
import com.alibaba.rsqldb.parser.creator.ChannelCreatorFactory;
import com.alibaba.rsqldb.parser.result.ScriptParseResult;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.channel.sink.ISink;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.metadata.MetaDataField;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.operator.impl.ScriptOperator;

/**
 * create sql的解析内容。主要完成channel的创建
 */
public class CreateSqlBuilder extends AbstractSqlBuilder {

    private static final Log LOG = LogFactory.getLog(CreateSqlBuilder.class);
    private static String TABLE_NAME = "sql_create_table_name";
    /**
     * 保存存储的字段结构
     */
    protected MetaData metaData = new MetaData();
    /**
     * 和property等价，把property转化成Properties
     */
    protected Properties properties;

    /**
     * sql中with部分的内容，主要是连接参数
     */
    // protected List<SqlNode> property;
    /**
     * 创建的channel，主要是输入逻辑
     */
    protected ISource<?> source;
    /**
     * key和整个属性sql的映射，主要用于修改sql时使用
     */
    protected Map<String, String> key2PropertyItem = new HashMap<>();
    protected WaterMark waterMark;
    /**
     * 需要覆盖的值，会覆盖sql中的值，主要用于生成sql和outchannel
     */
    @Deprecated
    protected Map<String, String> maskProperty = new HashMap<>();
    /**
     * 如果是规则引擎，可以build成channel
     */
    protected AtomicBoolean hasBuilder = new AtomicBoolean(false);
    private List<String> headerFieldNames;

    @Override
    public void build() {
        if (!hasBuilder.compareAndSet(false, true)) {
            return;
        }
        this.source = createSource();

        getPipelineBuilder().setSource(source);
        getPipelineBuilder().setChannelMetaData(metaData);
        if (StringUtil.isEmpty(this.source.getGroupName())) {
            this.source.setGroupName(DigestUtils.md5Hex(this.source.getName()));
        }
    }

    @Override
    public SqlBuilderResult buildSql() {
        build();
        PipelineBuilder builder = createPipelineBuilder();
        boolean hasBuilder = false;

        if (this.getScripts() != null && this.getScripts().size() > 0) {
            hasBuilder = true;
            ScriptParseResult scriptParseResult = new ScriptParseResult();
            scriptParseResult.setScriptValueList(this.getScripts());
            builder.addChainStage(new ScriptOperator(scriptParseResult.getScript()));
        }
        if (hasBuilder) {
            mergeSQLBuilderResult(new SqlBuilderResult(builder, this));
            SqlBuilderResult sqlBuilderResult = new SqlBuilderResult(this.pipelineBuilder);
            sqlBuilderResult.getStageGroup().setViewName("Shuffle MSG");
            sqlBuilderResult.getStageGroup().setSql("Shuffle MSG");
        }

        return new SqlBuilderResult(pipelineBuilder, this);
    }

    @Override
    public String getFieldName(String fieldName, boolean containsSelf) {
        return null;
    }

    public ISource<?> createSource() {
        if (this.source != null) {
            return this.source;
        }
        if (this.properties.size() == 0) {
            return null;
        }

        Properties properties = createProperty();
        properties.put(TABLE_NAME, getTableName());
        if (this.headerFieldNames != null) {
            properties.put("headerFieldNames", this.headerFieldNames);
        }

        properties.put("metaData", this.metaData);
        metaData.setTableName(getTableName());
        return ChannelCreatorFactory.createSource(pipelineBuilder.getPipelineNameSpace(), pipelineBuilder.getPipelineName(), properties, metaData);

    }

    private Properties createProperty() {
        Properties properties = new Properties();
        properties.putAll(this.properties);
        return properties;
    }

    public ISink<?> createSink() {
        if (this.properties.size() == 0) {
            return null;
        }
        Properties properties = createProperty();
        properties.put(TABLE_NAME, getTableName());
        metaData.setTableName(getTableName());
        return ChannelCreatorFactory.createSink(pipelineBuilder.getPipelineNameSpace(), pipelineBuilder.getPipelineName(), properties, metaData);

    }

    /**
     * 把输入格式形成retain脚本，便于输出只保留必须的字段
     *
     * @return
     */
    public String getRetainScript() {
        if (metaData == null) {
            throw new RuntimeException("expect metadata,but not " + toString());
        }
        String retainScript = "retainField(";
        List<MetaDataField> metadataFields = metaData.getMetaDataFields();
        boolean isFirst = true;
        for (MetaDataField metadataField : metadataFields) {
            if (isFirst) {
                isFirst = false;
            } else {
                retainScript = retainScript + ",";
            }
            retainScript = retainScript + metadataField.getFieldName();
        }
        retainScript = retainScript + ");";
        return retainScript;
    }

    @Override
    public String createSql() {
        String sql = super.createSql();
        if (StringUtil.isEmpty(sql) || maskProperty == null || maskProperty.size() == 0) {
            return sql;
        }
        for (Map.Entry<String, String> entry : maskProperty.entrySet()) {
            String key = entry.getKey();
            String oriValue = this.properties.getProperty(key);
            String dstValue = entry.getValue();
            String oriPropertyItem = key2PropertyItem.get(key);
            if (oriValue == null) {
                continue;
            }
            String dstPropertyItem = oriPropertyItem.replace(oriValue, dstValue);
            sql = sql.replace(oriPropertyItem, dstPropertyItem);
        }

        return sql;
    }

    public void putMaskProperty(String key, String value) {
        this.maskProperty.put(key, value);
    }

    @Override
    public Set<String> parseDependentTables() {
        return new HashSet<>();
    }

    public MetaData getMetaData() {
        return metaData;
    }

    public void setMetaData(MetaData metaData) {
        this.metaData = metaData;
    }

    public ISource getSource() {
        return source;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public List<String> getHeaderFieldNames() {
        return headerFieldNames;
    }

    public void setHeaderFieldNames(List<String> headerFieldNames) {
        this.headerFieldNames = headerFieldNames;
    }

    public WaterMark getWaterMark() {
        return waterMark;
    }

    public void setWaterMark(WaterMark waterMark) {
        this.waterMark = waterMark;
    }

    public Map<String, String> getKey2PropertyItem() {
        return key2PropertyItem;
    }

    public static class WaterMark {
        String timeFieldName;
        int waterMarkSecond;

        public String getTimeFieldName() {
            return timeFieldName;
        }

        public void setTimeFieldName(String timeFieldName) {
            this.timeFieldName = timeFieldName;
        }

        public int getWaterMarkSecond() {
            return waterMarkSecond;
        }

        public void setWaterMarkSecond(int waterMarkSecond) {
            this.waterMarkSecond = waterMarkSecond;
        }
    }
}
