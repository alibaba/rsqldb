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

import org.apache.calcite.sql.SqlNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.common.model.NameCreator;
import org.apache.rocketmq.streams.common.topology.ChainStage;
import org.apache.rocketmq.streams.common.topology.builder.IStageBuilder;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.common.topology.stages.JoinChainStage;
import org.apache.rocketmq.streams.common.topology.stages.RightJoinChainStage;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.filter.builder.ExpressionBuilder;
import org.apache.rocketmq.streams.filter.function.expression.Equals;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;
import org.apache.rocketmq.streams.filter.operator.expression.RelationExpression;
import org.apache.rocketmq.streams.script.operator.impl.ScriptOperator;
import com.alibaba.rsqldb.parser.parser.result.BuilderParseResult;
import com.alibaba.rsqldb.parser.parser.result.IParseResult;
import com.alibaba.rsqldb.parser.parser.result.NotSupportParseResult;
import org.apache.rocketmq.streams.window.builder.WindowBuilder;
import org.apache.rocketmq.streams.window.operator.join.JoinWindow;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 解析join： 1.维表join 2.双流join，解析成左右两个pipline，通过msgfromsource区分数据来源，两个pipline共享一个window，来实现数据汇聚
 */
public class JoinSQLBuilder extends SelectSQLBuilder {

    private static final Log LOG = LogFactory.getLog(JoinSQLBuilder.class);

    protected IParseResult left;//左表

    protected IParseResult right;//右表

    protected String joinType;//join类型：值此号inner join和left join

    protected String onCondition;//join 的条件

    protected SqlNode conditionSQLNode;//join条件对应的sql

    protected String rightPiplineName;//默认是空，在双流join场景，左右流都可能填充这个值


    protected boolean needWhereToCondition=false;//need where as onCondition

    @Override
    public void build() {
        boolean isJoin = isJoin();//判断是否是大流join，目前只支持自己join自己
        if (isJoin) {
            //左表build会产生脚本，脚本设置到pipline中
            if (getScripts() != null && getScripts().size() > 0) {
                StringBuilder stringBuilder = new StringBuilder();
                for (String script : scripts) {
                    stringBuilder.append(script + ";");
                }
                getPipelineBuilder().addChainStage(new ScriptOperator(stringBuilder.toString()));
            }
            buildJoin();
            return;
        }
        /**
         * 下面的场景是维表join或LateralTable join的场景
         */
        if (left != null && left instanceof BuilderParseResult) {
            BuilderParseResult result = (BuilderParseResult)left;
            result.getBuilder().setPipelineBuilder(pipelineBuilder);
            //result.getBuilder().setTreeSQLBulider(getTreeSQLBulider());
            result.getBuilder().setTableName2Builders(getTableName2Builders());
            result.getBuilder().buildSQL();
        }

        //左表build会产生脚本，脚本设置到pipline中
        if (getScripts() != null && getScripts().size() > 0) {
            StringBuilder stringBuilder = new StringBuilder();
            for (String script : scripts) {
                stringBuilder.append(script + ";");
            }
            getPipelineBuilder().addChainStage(new ScriptOperator(stringBuilder.toString()));
        }
        if (right != null && BuilderParseResult.class.isInstance(right) && NotSupportParseResult.class.isInstance(right)
            == false) {
            BuilderParseResult result = (BuilderParseResult)right;
            AbstractSQLBuilder builder = result.getBuilder();
            builder.setPipelineBuilder(pipelineBuilder);
            // builder.setTreeSQLBulider(getTreeSQLBulider());
            builder.setTableName2Builders(getTableName2Builders());
            builder.addRootTableName(this.rootTableNames);
            if (SnapshotBuilder.class.isInstance(builder)) {
                SnapshotBuilder snapshotBuilder = (SnapshotBuilder)builder;
                //snapshotBuilder.setExpression(onCondition);
                snapshotBuilder.buildDimCondition(conditionSQLNode,joinType,onCondition);
            }else {
                builder.buildSQL();
            }

        }

    }

    /**
     * 对于双流join的builder
     */
    protected void buildJoin() {
        if (isRightBranch(pipelineBuilder.getParentTableName())) {
            pipelineBuilder.addChainStage(new IStageBuilder<ChainStage>() {

                @Override
                public ChainStage createStageChain(PipelineBuilder pipelineBuilder) {
                    RightJoinChainStage chainStage = new RightJoinChainStage();
                    chainStage.setPipelineName(createOrGetRightPiplineName());
                    return chainStage;
                }

                @Override
                public void addConfigurables(PipelineBuilder pipelineBuilder) {

                }
            });
            pipelineBuilder.setBreak(true);
        } else {
            List<IConfigurable> configurableList = new ArrayList<>();
            String piplineNamespace = pipelineBuilder.getPipelineNameSpace();
            String piplineName = pipelineBuilder.getPipelineName();
            BuilderParseResult leftResult = (BuilderParseResult)left;
            BuilderParseResult rightResult = (BuilderParseResult)right;

            //创建共享的窗口，左右pipline 通过输出到共享的窗口完成数据汇聚
            JoinWindow joinWindow = WindowBuilder.createDefaultJoinWindow();
            joinWindow.setNameSpace(piplineNamespace);
            joinWindow.setConfigureName(NameCreator.createNewName(piplineName, "join", "window"));

            AtomicBoolean hasNoEqualsExpression = new AtomicBoolean(false);//是否有非等值的join 条件
            Map<String, String> left2Right = createJoinFieldsFromCondition(onCondition, hasNoEqualsExpression);//把等值条件的左右字段映射成map
            List<String> leftList = new ArrayList<>();
            List<String> rightList = new ArrayList<>();
            leftList.addAll(left2Right.keySet());
            rightList.addAll(left2Right.values());
            joinWindow.setLeftJoinFieldNames(leftList);
            joinWindow.setRightJoinFieldNames(rightList);

            joinWindow.setRightAsName(rightResult.getBuilder().getAsName());
            joinWindow.setJoinType(joinType);
            //如果有非等值，则把这个条件设置进去
            if (hasNoEqualsExpression.get()) {
                joinWindow.setExpression(onCondition);
            }
            pipelineBuilder.addConfigurables(joinWindow);
            //这个分支对应的数据来源。key：数据来源的tablename，value：pipline。在解析阶段应用
            Map<String, String> tableName2PiplineNames = new HashMap<>();
            /**
             * join的left流生成一个pipline
             */
            String leftPiplineName = NameCreator.createNewName("subpipline", piplineName, "join", "left");
            PipelineBuilder leftPipelineBuilder = new PipelineBuilder(piplineNamespace, leftPiplineName);

            leftResult.getBuilder().setPipelineBuilder(leftPipelineBuilder);
            //result.getBuilder().setTreeSQLBulider(getTreeSQLBulider());
            leftResult.getBuilder().setTableName2Builders(getTableName2Builders());
            leftResult.getBuilder().buildSQL();
            leftPipelineBuilder.addChainStage(joinWindow);
            tableName2PiplineNames.put(leftResult.getBuilder().getTableName(), leftPipelineBuilder.getPipeline().getConfigureName());
            configurableList.addAll(leftPipelineBuilder.getConfigurables());

            /**
             * join的rigth流生成一个pipline
             */
            String rightPiplineName = createOrGetRightPiplineName();
            PipelineBuilder rightBuilder = new PipelineBuilder(piplineNamespace, rightPiplineName);
            rightResult.getBuilder().setPipelineBuilder(rightBuilder);
            rightResult.getBuilder().setTableName2Builders(getTableName2Builders());
            rightResult.getBuilder().buildSQL();
            rightBuilder.addChainStage(joinWindow);
            configurableList.addAll(rightBuilder.getConfigurables());
            tableName2PiplineNames.put(rightResult.getBuilder().getTableName(), rightBuilder.getPipeline().getConfigureName());

            /**
             * 增加一个join stage，里面有左右pipline
             */
            pipelineBuilder.addChainStage(new IStageBuilder<ChainStage>() {

                @Override
                public ChainStage createStageChain(PipelineBuilder pipelineBuilder) {
                    JoinChainStage joinChainStage = new JoinChainStage();
                    joinChainStage.setWindow(joinWindow);
                    joinChainStage.setLeftPipline(leftPipelineBuilder.getPipeline());
                    joinChainStage.setRightPipline(rightBuilder.getPipeline());
                    joinChainStage.setRigthDependentTableName(((BuilderParseResult)right).getBuilder().getTableName());
                    return joinChainStage;
                }

                @Override
                public void addConfigurables(PipelineBuilder pipelineBuilder) {
                    pipelineBuilder.addConfigurables(configurableList);
                }
            });
        }

    }

    /**
     * 当前解析的流是否是右流join
     *
     * @return
     */
    protected boolean isRightBranch(String parentName) {
        if (!isJoin()) {
            return false;
        }
        if (this.rootTableNames.size() <= 1) {
            return false;
        }
        BuilderParseResult rightResult = (BuilderParseResult) getRight();
        if( rightResult.getBuilder().getTableName().equals(parentName)){
            return true;
        }
        return false;
//        BuilderParseResult rightResult = (BuilderParseResult)getRight();
//        return rightResult.getBuilder().getTableName().equals(parentName);
    }

    /**
     * right pipline name
     *
     * @return
     */
    public String createOrGetRightPiplineName() {
        if (StringUtil.isEmpty(rightPiplineName)) {
            String piplineName = pipelineBuilder.getPipelineName();
            rightPiplineName = NameCreator.createNewName("subpipline", piplineName, "join", "right");
        }
        return rightPiplineName;

    }

    /**
     * 从条件中找到join 左右的字段。如果有非等值，则不包含在内
     *
     * @param
     * @param onCondition
     * @return
     */
    public Map<String, String> createJoinFieldsFromCondition(String onCondition, AtomicBoolean hasNoEqualsExpression) {
        List<Expression> expressions = new ArrayList<>();
        List<RelationExpression> relationExpressions = new ArrayList<>();
        ExpressionBuilder.createOptimizationExpression("tmp", "tmp", onCondition, expressions, relationExpressions);
        Map<String, String> left2Right = new HashMap<>();
        for (Expression expression : expressions) {
            String varName = expression.getVarName();
            String valueName = expression.getValue().toString();
            if (!Equals.isEqualFunction(expression.getFunctionName())) {
                hasNoEqualsExpression.set(true);
                continue;
            }
            if (expression.getVarName().equals(expression.getValue().toString())) {
                left2Right.put(valueName, valueName);
            } else {
                String leftName = findByField(getJoinBuilder(left), varName);
                if (leftName == null) {
                    leftName = getJoinBuilder(right).getFieldName(varName);
                    if (leftName == null) {
                        throw new RuntimeException("parser join condition error " + onCondition);
                    }
                    left2Right.put(valueName, varName);
                } else {
                    left2Right.put(varName, valueName);
                }
            }
        }
        return left2Right;
    }

    /**
     * 判断varName这个字段是否在builder中，返回标准格式（主流不带别名，子表带别名）
     *
     * @param builder
     * @param varName
     * @return
     */
    protected String findByField(AbstractSQLBuilder builder, String varName) {
        if (SelectSQLBuilder.class.isInstance(builder)) {
            SelectSQLBuilder selectSQLBuilder = (SelectSQLBuilder)builder;
            String value = selectSQLBuilder.findFieldBySelect(selectSQLBuilder, varName);
            if (value != null) {
                return value;
            }
        }
        return builder.getFieldName(varName);
    }

    /**
     * 判断是否是双流join，目前只支持自己join自己
     *
     * @return
     */
    public boolean isJoin() {

        AbstractSQLBuilder leftBuilder = getJoinBuilder(left);
        AbstractSQLBuilder rightBuilder = getJoinBuilder(right);

        if (leftBuilder == null || rightBuilder == null) {
            return false;
        }
        if (SnapshotBuilder.class.isInstance(rightBuilder) || LateralTableBuilder.class.isInstance(rightBuilder)) {
            return false;
        }
        return true;
    }

    /**
     * 判断是否是双流join，目前只支持自己join自己
     *
     * @return
     */
    private AbstractSQLBuilder getJoinBuilder(IParseResult result) {
        if (result != null && BuilderParseResult.class.isInstance(result) && NotSupportParseResult.class.isInstance(result)
            == false) {
            BuilderParseResult parseResult = (BuilderParseResult)result;
            return parseResult.getBuilder();
        } else {
            return null;
        }
    }

    @Override
    public Set<String> getAllFieldNames() {
        return getAllFieldNames(null);
    }

    /**
     * 获取所有字段名，需要特殊处理*号。左表不带别名，右表必带别名
     *
     * @return
     */
    public Set<String> getAllFieldNames(String aliasName) {
        Set<String> fields = new HashSet<>();
        if (left != null && BuilderParseResult.class.isInstance(left)) {
            BuilderParseResult result = (BuilderParseResult)left;
            if (SelectSQLBuilder.class.isInstance(result.getBuilder())) {
                SelectSQLBuilder selectSQLBuilder = (SelectSQLBuilder)result.getBuilder();
                if (aliasName == null || (aliasName != null && aliasName.equals(selectSQLBuilder.getAsName()))) {
                    fields.addAll(selectSQLBuilder.getAllFieldNames());
                }

            }

        }
        if (right != null && BuilderParseResult.class.isInstance(right)) {
            BuilderParseResult result = (BuilderParseResult)right;
            if (SelectSQLBuilder.class.isInstance(result.getBuilder())) {
                SelectSQLBuilder selectSQLBuilder = (SelectSQLBuilder)result.getBuilder();
                if (aliasName == null || (aliasName != null && aliasName.equals(selectSQLBuilder.getAsName()))) {
                    String asName = selectSQLBuilder.getAsName();
                    if (asName == null) {
                        asName = "";
                    } else {
                        asName = asName + ".";
                    }
                    for (String fieldName : selectSQLBuilder.getAllFieldNames()) {
                        fields.add(asName + fieldName);
                    }
                }

            }
        }
        return fields;
    }

    @Override
    public Set<String> parseDependentTables() {
        Set<String> dependentTables = new HashSet<>();
        if (left != null && BuilderParseResult.class.isInstance(left)) {
            BuilderParseResult result = (BuilderParseResult)left;
            //  result.getBuilder().setTreeSQLBulider(getTreeSQLBulider());
            dependentTables.addAll(result.getBuilder().parseDependentTables());
        }
        if (right != null && BuilderParseResult.class.isInstance(right)) {
            BuilderParseResult result = (BuilderParseResult)right;
            AbstractSQLBuilder rightBuilder = result.getBuilder();
            if (SnapshotBuilder.class.isInstance(rightBuilder) || LateralTableBuilder.class.isInstance(rightBuilder)) {
                return dependentTables;
            }
            // result.getBuilder().setTreeSQLBulider(getTreeSQLBulider());
            dependentTables.addAll(result.getBuilder().parseDependentTables());
        }
        return dependentTables;
    }

    public IParseResult getLeft() {
        return left;
    }

    /**
     * 根据别名，返回全部字段
     *
     * @param aliasName
     * @param parseResult
     * @return
     */
    protected Set<String> findFieldsByAliasName(String aliasName, IParseResult parseResult) {
        if (BuilderParseResult.class.isInstance(parseResult)) {
            AbstractSQLBuilder builder = ((BuilderParseResult)getLeft()).getBuilder();
            if (!builder.getAsName().equals(aliasName)) {
                return null;
            }
            if (SelectSQLBuilder.class.isInstance(builder)) {
                SelectSQLBuilder selectSQLBuilder = (SelectSQLBuilder)builder;
                return selectSQLBuilder.getAllFieldNames();
            }

        }
        return null;
    }

    @Override
    public String getFieldName(String fieldName) {
        String oriFieldName = fieldName;
        String value = null;
        String asName = null;
        int index = fieldName.indexOf(".");
        if (index != -1) {
            asName = fieldName.substring(0, index);
            fieldName = fieldName.substring(index + 1);
        }
        String tableAsName=null;
        if (BuilderParseResult.class.isInstance(getLeft())) {
            BuilderParseResult builderParseResult = (BuilderParseResult)getLeft();
            tableAsName=builderParseResult.getBuilder().getAsName();
            if(asName!=null&&tableAsName==null){
                tableAsName=builderParseResult.getBuilder().getTableName();
            }
            if ((asName != null && asName.equals(tableAsName)) | StringUtil.isEmpty(asName)) {
                if (SelectSQLBuilder.class.isInstance(builderParseResult.getBuilder())) {
                    SelectSQLBuilder selectSQLBuilder = (SelectSQLBuilder)builderParseResult.getBuilder();
                    value = selectSQLBuilder.getFieldName(fieldName, true);
                }
                if (StringUtil.isNotEmpty(value)) {
                    return value;
                }
            }
        }
        if (BuilderParseResult.class.isInstance(getRight())) {
            BuilderParseResult builderParseResult = (BuilderParseResult)getRight();
            tableAsName=builderParseResult.getBuilder().getAsName();
            if(asName!=null&&tableAsName==null){
                tableAsName=builderParseResult.getBuilder().getTableName();
            }
            if ((asName != null && asName.equals(tableAsName)) | StringUtil.isEmpty(asName)) {
                if (SelectSQLBuilder.class.isInstance(builderParseResult.getBuilder())) {
                    SelectSQLBuilder selectSQLBuilder = (SelectSQLBuilder)builderParseResult.getBuilder();
                    value = selectSQLBuilder.getFieldName(fieldName, true);
                    if (StringUtil.isNotEmpty(value)) {
                        if (value.indexOf(".") != -1) {
                            return value;
                        }
                        String aliasName = builderParseResult.getBuilder().getAsName() == null ? "" : builderParseResult.getBuilder().getAsName() + ".";
                        return aliasName + value;
                    }

                }

            }
        }
        return null;

    }

    public boolean isNeedWhereToCondition() {
        return needWhereToCondition;
    }

    public void setNeedWhereToCondition(boolean needWhereToCondition) {
        this.needWhereToCondition = needWhereToCondition;
    }

    public void setLeft(IParseResult left) {
        this.left = left;
    }

    public IParseResult getRight() {
        return right;
    }

    public void setRight(IParseResult right) {
        this.right = right;
    }

    public String getJoinType() {
        return joinType;
    }

    public void setJoinType(String joinType) {
        this.joinType = joinType;
    }

    public String getOnCondition() {
        return onCondition;
    }

    public void setOnCondition(String onCondition) {
        this.onCondition = onCondition;
    }

    public SqlNode getConditionSQLNode() {
        return conditionSQLNode;
    }

    public void setConditionSQLNode(SqlNode conditionSQLNode) {
        this.conditionSQLNode = conditionSQLNode;
    }
}
