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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.rocketmq.streams.common.channel.sink.ISink;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.metadata.MetaDataField;
import org.apache.rocketmq.streams.common.utils.ContantsUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.operator.impl.ScriptOperator;
import com.alibaba.rsqldb.parser.parser.builder.channel.ChannelCreatorFactory;
import com.alibaba.rsqldb.parser.parser.result.ScriptParseResult;
import com.alibaba.rsqldb.parser.util.ColumnUtil;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlProperty;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * create sql的解析内容。主要完成channel的创建
 */
public class CreateSQLBuilder extends AbstractSQLBuilder<AbstractSQLBuilder> {

    private static final Log LOG = LogFactory.getLog(CreateSQLBuilder.class);
    private static String TABLE_NAME="sql_create_table_name";

    protected MetaData metaData = new MetaData();//保存存储的字段结构
    protected List<SqlNode> property;//sql中with部分的内容，主要是连接参数
    protected Properties properties;//和property等价，把property转化成Properties
    protected ISource source;//创建的channel，主要是输入逻辑
    public static final String PREX = "blink_ruleengine";
    protected Map<String, String> key2PropertyItem = new HashMap<>();//key和整个属性sql的映射，主要用于修改sql时使用
    protected Map<String, String> maskProperty = new HashMap<>();//需要覆盖的值，会覆盖sql中的值，主要用于生成sql和outchannel
    /**
     * 如果是规则引擎，可以build成channel
     */
    protected AtomicBoolean hasBuilder = new AtomicBoolean(false);

    @Override
    public void build() {
        if (!hasBuilder.compareAndSet(false, true)) {
            return;
        }
        this.source = createSource();
        this.source.setGroupName(getPipelineBuilder().getPipelineName());
        getPipelineBuilder().setSource(source);
        getPipelineBuilder().setChannelMetaData(metaData);
        if(this.getScripts()!=null&&this.getScripts().size()>0){
            ScriptParseResult scriptParseResult=new ScriptParseResult();
            scriptParseResult.setScriptValueList(this.getScripts());
            getPipelineBuilder().addChainStage(new ScriptOperator(scriptParseResult.getScript()));
        }
        //this.outputChannel=createOutputChannel(maskProperty);
    }

    @Override
    public String getFieldName(String fieldName, boolean containsSelf) {
        return null;
    }

    public ISource createSource() {
        if (this.source != null) {
            return this.source;
        }
        if (property == null) {
            return null;
        }

        this.properties = createProperty();
        this.properties.put(TABLE_NAME,getTableName());
        ISource source = ChannelCreatorFactory.createSource(pipelineBuilder.getPipelineNameSpace(),
            pipelineBuilder.getPipelineName(), properties, metaData);

        return source;

    }

    public ISink createSink() {

        if (property == null) {
            return null;
        }

        this.properties = createProperty();
        this.properties.put(TABLE_NAME,getTableName());
        ISink sink = ChannelCreatorFactory.createSink(pipelineBuilder.getPipelineNameSpace(),
            pipelineBuilder.getPipelineName(), properties, metaData);

        return sink;

    }

    public Properties createProperty() {
        /**
         * 把property的值，生成key：value。和key：propertysql的值
         */
        Properties properties = new Properties();
        SqlProperty sqlProperty = null;
        String type = "sls";
        int i = 0;
        for (SqlNode sqlNode : this.property) {
            if (!SqlProperty.class.isInstance(sqlNode)) {
                i++;
                continue;
            }
            SqlProperty property = (SqlProperty)sqlNode;
            String value = property.getValueString();
            if (ContantsUtil.isContant(value)) {
                value = value.substring(1, value.length() - 1);
            }
            String key=property.getKeyString().toLowerCase();
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
     * 创建输出的channel
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
    public String createSQL() {
        String sql = super.createSQL();
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

    @Override
    public Set<String> parseDependentTables() {
        return new HashSet<>();
    }

    /**
     * 把创建表的语句转换成metadata
     *
     * @param sqlNodes
     */
    public void createColumn(SqlNodeList sqlNodes) {
        MetaData metaData = ColumnUtil.createMetadata(this,sqlNodes);
        this.metaData = metaData;
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
}
