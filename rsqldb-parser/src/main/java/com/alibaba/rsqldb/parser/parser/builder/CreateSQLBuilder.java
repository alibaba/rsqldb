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

import com.alibaba.rsqldb.parser.parser.SQLBuilderResult;
import com.alibaba.rsqldb.parser.parser.builder.channel.ChannelCreatorFactory;
import com.alibaba.rsqldb.parser.parser.result.ScriptParseResult;
import com.alibaba.rsqldb.parser.util.ColumnUtil;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlProperty;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.channel.sink.ISink;
import org.apache.rocketmq.streams.common.channel.source.AbstractSource;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.metadata.MetaDataField;
import org.apache.rocketmq.streams.common.topology.ChainStage;
import org.apache.rocketmq.streams.common.topology.builder.IStageBuilder;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.common.topology.stages.ShuffleConsumerChainStage;
import org.apache.rocketmq.streams.common.topology.stages.ShuffleProducerChainStage;
import org.apache.rocketmq.streams.common.utils.ContantsUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.operator.impl.ScriptOperator;

/**
 * create sql??????????????????????????????channel?????????
 */
public class CreateSQLBuilder extends AbstractSQLBuilder<AbstractSQLBuilder> {

    private static final Log LOG = LogFactory.getLog(CreateSQLBuilder.class);
    private static String TABLE_NAME = "sql_create_table_name";
    private List<String> headerFieldNames;
    /**
     * ???????????????????????????
     */
    protected MetaData metaData = new MetaData();
    /**
     * sql???with???????????????????????????????????????
     */
    protected List<SqlNode> property;
    /**
     * ???property????????????property?????????Properties
     */
    protected Properties properties;
    /**
     * ?????????channel????????????????????????
     */
    protected ISource<?> source;
    /**
     * key???????????????sql??????????????????????????????sql?????????
     */
    protected Map<String, String> key2PropertyItem = new HashMap<>();
    /**
     * ??????????????????????????????sql??????????????????????????????sql???outchannel
     */
    protected Map<String, String> maskProperty = new HashMap<>();
    /**
     * ??????????????????????????????build???channel
     */
    protected AtomicBoolean hasBuilder = new AtomicBoolean(false);

    @Override public void build() {
        if (!hasBuilder.compareAndSet(false, true)) {
            return;
        }
        this.source = createSource();
        if (StringUtil.isEmpty(this.source.getGroupName())) {
            this.source.setGroupName(StringUtil.getUUID());
        }
        getPipelineBuilder().setSource(source);
        getPipelineBuilder().setChannelMetaData(metaData);


        //this.outputChannel=createOutputChannel(maskProperty);
    }

    @Override public SQLBuilderResult buildSql() {
        build();
        PipelineBuilder builder=createPipelineBuilder();
        boolean hasBuilder=false;
        if(AbstractSource.class.isInstance(this.source)){

            AbstractSource abstractSource=(AbstractSource)this.source;
            if(abstractSource.getShuffleConcurrentCount()>0){
                hasBuilder=true;
                final ChainStage<?> producerChainStage = new ShuffleProducerChainStage();
                ((ShuffleProducerChainStage) producerChainStage).setShuffleOwnerName(MapKeyUtil.createKey(this.getPipelineBuilder().getPipelineNameSpace(),this.getPipelineBuilder().getPipelineName(),this.getPipelineBuilder().getPipeline().getChannelName()));
                ((ShuffleProducerChainStage) producerChainStage).setSplitCount(abstractSource.getShuffleConcurrentCount());
                builder.addChainStage(new IStageBuilder<ChainStage>() {
                    @Override public ChainStage createStageChain(PipelineBuilder pipelineBuilder) {
                        return producerChainStage;
                    }

                    @Override public void addConfigurables(PipelineBuilder pipelineBuilder) {

                    }
                });

                final ChainStage<?> consumerChainStage = new ShuffleConsumerChainStage<>();
                ((ShuffleConsumerChainStage) consumerChainStage).setShuffleOwnerName(MapKeyUtil.createKey(this.getPipelineBuilder().getPipelineNameSpace(),this.getPipelineBuilder().getPipelineName(),this.getPipelineBuilder().getPipeline().getChannelName()));
                builder.addChainStage(new IStageBuilder<ChainStage>() {
                    @Override public ChainStage createStageChain(PipelineBuilder pipelineBuilder) {
                        return consumerChainStage;
                    }

                    @Override public void addConfigurables(PipelineBuilder pipelineBuilder) {

                    }
                });

            }
        }


        if (this.getScripts() != null && this.getScripts().size() > 0) {
            hasBuilder=true;
            ScriptParseResult scriptParseResult = new ScriptParseResult();
            scriptParseResult.setScriptValueList(this.getScripts());
            builder.addChainStage(new ScriptOperator(scriptParseResult.getScript()));
        }
        if(hasBuilder){
            mergeSQLBuilderResult(new SQLBuilderResult(builder,this));
            SQLBuilderResult sqlBuilderResult= new SQLBuilderResult(this.pipelineBuilder);
            sqlBuilderResult.getStageGroup().setViewName("Shuffle MSG");
            sqlBuilderResult.getStageGroup().setSql("Shuffle MSG");
        }
        SQLBuilderResult sqlBuilderResult=  new SQLBuilderResult(pipelineBuilder,this);

        return sqlBuilderResult;
    }

    @Override public String getFieldName(String fieldName, boolean containsSelf) {
        return null;
    }

    public ISource<?> createSource() {
        if (this.source != null) {
            return this.source;
        }
        if (property == null) {
            return null;
            }

        this.properties = createProperty();
        this.properties.put(TABLE_NAME, getTableName());
        this.properties.put("headerFieldNames", this.headerFieldNames);
        this.properties.put("metaData", this.metaData);
        this.properties.put("fieldDelimiter",",");

        return ChannelCreatorFactory.createSource(pipelineBuilder.getPipelineNameSpace(), pipelineBuilder.getPipelineName(), properties, metaData);

    }

    public ISink<?> createSink() {
        if (property == null) {
            return null;
        }
        this.properties = createProperty();
        this.properties.put(TABLE_NAME, getTableName());
        return ChannelCreatorFactory.createSink(pipelineBuilder.getPipelineNameSpace(), pipelineBuilder.getPipelineName(), properties, metaData);

    }

    public Properties createProperty() {
        //???property???????????????key???value??????key???propertysql??????
        Properties properties = new Properties();
        SqlProperty sqlProperty = null;
        String type = "sls";
        int i = 0;
        for (SqlNode sqlNode : this.property) {
            if (!SqlProperty.class.isInstance(sqlNode)) {
                i++;
                continue;
            }
            SqlProperty property = (SqlProperty) sqlNode;
            String value = property.getValueString();
            if (ContantsUtil.isContant(value)) {
                value = value.substring(1, value.length() - 1);
            }
            String key = property.getKeyString().toLowerCase();
            properties.put(property.getKeyString(), value);
            properties.put(key, value);
            if ("type".equals(key)) {
                type = property.getValueString();
            }

            key2PropertyItem.put(property.getKeyString(), property.toString());
            key2PropertyItem.put(key, property.toString());
            i++;
        }
        return properties;
    }

    /**
     * ???????????????channel
     * @param newValue
     * @return
     */
    //    public IChannel createOutputChannel(Map<String,String> newValue){
    //        Properties properties=new Properties();
    //        properties.putAll(this.properties);
    //        properties.putAll(newValue);
    //        IChannel outputChannel= ChannelCreatorFactory.create(piplineCreator.getNamespace(),piplineCreator
    //        .getName(),properties);
    //        return outputChannel;
    //    }

    /**
     * ?????????????????????retain?????????????????????????????????????????????
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

    @Override public String createSql() {
        String sql = super.createSql();
        if (StringUtil.isEmpty(sql) || maskProperty == null || maskProperty.size() == 0) {
            return sql;
        }
        Iterator<Map.Entry<String, String>> it = maskProperty.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, String> entry = it.next();
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

    @Override public Set<String> parseDependentTables() {
        return new HashSet<>();
    }

    /**
     * ??????????????????????????????metadata
     *
     * @param sqlNodes
     */
    public void createColumn(SqlNodeList sqlNodes) {
        List<String> headerFieldNames = new ArrayList<>();
        MetaData metaData = ColumnUtil.createMetadata(this, sqlNodes, headerFieldNames);
        this.metaData = metaData;
        this.headerFieldNames = headerFieldNames;
    }

    public List<SqlNode> getProperty() {
        return property;
    }

    public void setProperty(List<SqlNode> property) {
        this.property = property;
    }

    public MetaData getMetaData() {
        return metaData;
    }

    public ISource getSource() {
        return source;
    }

    public Properties getProperties() {
        return properties;
    }

    public List<String> getHeaderFieldNames() {
        return headerFieldNames;
    }
}
