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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import com.alibaba.rsqldb.parser.SqlBuilderResult;
import com.alibaba.rsqldb.parser.result.BuilderParseResult;
import com.alibaba.rsqldb.parser.result.IParseResult;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.model.NameCreatorContext;
import org.apache.rocketmq.streams.common.topology.IStageBuilder;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.common.topology.model.AbstractChainStage;
import org.apache.rocketmq.streams.common.topology.model.AbstractStage;
import org.apache.rocketmq.streams.common.topology.stages.EmptyChainStage;
import org.apache.rocketmq.streams.common.topology.stages.JoinStartChainStage;
import org.apache.rocketmq.streams.common.topology.stages.ShuffleChainStage;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.filter.builder.ExpressionBuilder;
import org.apache.rocketmq.streams.filter.function.expression.Equals;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;
import org.apache.rocketmq.streams.filter.operator.expression.RelationExpression;
import org.apache.rocketmq.streams.script.operator.impl.ScriptOperator;
import org.apache.rocketmq.streams.window.builder.WindowBuilder;
import org.apache.rocketmq.streams.window.operator.join.JoinWindow;

/**
 * 解析join： 1.维表join 2.双流join，解析成左右两个pipline，通过msgfromsource区分数据来源，两个pipline共享一个window，来实现数据汇聚
 */
public class JoinSqlBuilder extends SelectSqlBuilder {

    private static final Log LOG = LogFactory.getLog(JoinSqlBuilder.class);
    /**
     * 左表
     */
    protected IParseResult<?> left;
    /**
     * 右表
     */
    protected IParseResult<?> right;

    /**
     * join类型：值此号inner join和left join
     */
    protected String joinType;
    /**
     * join 的条件
     */
    protected String onCondition;

    //    /**
    //     * join条件对应的sql
    //     */
    //    protected SqlNode conditionSqlNode;

    /**
     * 默认是空，在双流join场景，window 的name
     */
    protected String joinWindowName;

    /**
     * need where as onCondition
     */
    protected boolean needWhereToCondition = false;
    private JoinWindow joinWindow;

    @Override
    public SqlBuilderResult buildSql() {
        if (left instanceof BuilderParseResult) {
            ((BuilderParseResult)left).getBuilder().setConfiguration(getConfiguration());
        }
        if (right instanceof BuilderParseResult) {
            ((BuilderParseResult)right).getBuilder().setConfiguration(getConfiguration());
        }
        //判断是否是大流join，目前只支持自己join自己
        boolean isJoin = isJoin();
        if (isJoin) {
            //左表build会产生脚本，脚本设置到pipline中
            if (getScripts() != null && getScripts().size() > 0) {
                StringBuilder stringBuilder = new StringBuilder();
                for (String script : scripts) {
                    stringBuilder.append(script).append(";");
                }
                AbstractChainStage chainStage = getPipelineBuilder().addChainStage(new ScriptOperator(stringBuilder.toString()));
                this.pipelineBuilder.setHorizontalStages(chainStage);
                this.pipelineBuilder.setCurrentChainStage(chainStage);
            }
            buildJoin();
            SqlBuilderResult sqlBuilderResult = new SqlBuilderResult(this.pipelineBuilder, pipelineBuilder.getFirstStages().get(0), pipelineBuilder.getCurrentChainStage());
            if (sqlBuilderResult.getStageGroup().getSql() == null) {
                sqlBuilderResult.getStageGroup().setSql(createSQLFromParser());
                sqlBuilderResult.getStageGroup().setViewName(createSQLFromParser());
            }
            return sqlBuilderResult;
        }

        //下面的场景是维表join或LateralTable join的场景
        if (left != null && left instanceof BuilderParseResult) {
            PipelineBuilder dimJoinPipelineBuilder = createPipelineBuilder();
            BuilderParseResult result = (BuilderParseResult)left;
            result.getBuilder().setPipelineBuilder(dimJoinPipelineBuilder);
            result.getBuilder().setTableName2Builders(getTableName2Builders());
            result.getBuilder().setConfiguration(getConfiguration());
            SqlBuilderResult sqlBuilderResult = result.getBuilder().buildSql();
            mergeSQLBuilderResult(sqlBuilderResult);
        }

        //左表build会产生脚本，脚本设置到pipline中
        if (getScripts() != null && getScripts().size() > 0) {
            StringBuilder stringBuilder = new StringBuilder();
            for (String script : scripts) {
                stringBuilder.append(script).append(";");
            }
            AbstractChainStage chainStage = getPipelineBuilder().addChainStage(new ScriptOperator(stringBuilder.toString()));
            chainStage.setSql(this.joinConditionSql);
            this.pipelineBuilder.setHorizontalStages(chainStage);
            this.pipelineBuilder.setCurrentChainStage(chainStage);
        }
        if (right != null && right instanceof BuilderParseResult) {
            SqlBuilderResult sqlBuilderResult = null;
            PipelineBuilder rightPipelineBuilder = createPipelineBuilder();
            BuilderParseResult result = (BuilderParseResult)right;
            AbstractSqlBuilder builder = result.getBuilder();
            builder.setPipelineBuilder(rightPipelineBuilder);
            builder.setTableName2Builders(getTableName2Builders());
            builder.addRootTableName(this.rootTableNames);
            if (builder instanceof SnapshotBuilder) {
                SnapshotBuilder snapshotBuilder = (SnapshotBuilder)builder;
                snapshotBuilder.buildDimCondition(onCondition, joinType);
                sqlBuilderResult = new SqlBuilderResult(rightPipelineBuilder, builder);
            } else {
                sqlBuilderResult = builder.buildSql();
            }
            mergeSQLBuilderResult(sqlBuilderResult);

        }
        SqlBuilderResult sqlBuilderResult = new SqlBuilderResult(this.pipelineBuilder, this.pipelineBuilder.getFirstStages().get(0), this.pipelineBuilder.getCurrentChainStage());

        if (sqlBuilderResult.getStageGroup().getSql() == null) {
            sqlBuilderResult.getStageGroup().setSql(this.sqlNode.toString());
            sqlBuilderResult.getStageGroup().setViewName(createSQLFromParser());
        }
        return sqlBuilderResult;
    }

    @Override
    public void build() {
        buildSql();

    }

    /**
     * 对于双流join的builder
     */
    protected void buildJoin() {
        String piplineNamespace = pipelineBuilder.getPipelineNameSpace();
        String piplineName = pipelineBuilder.getPipelineName();
        BuilderParseResult leftResult = (BuilderParseResult)left;
        BuilderParseResult rightResult = (BuilderParseResult)right;
        if (isRightBranch(pipelineBuilder.getParentTableName())) {

            JoinWindow joinWindow = getOrCreateJoinWindow(piplineNamespace, piplineName, rightResult.getBuilder().getAsName());
            SqlBuilderResult right = buildSQLBuilder(rightResult.getBuilder(), piplineNamespace, piplineName, joinWindow, false);
            JoinStartChainStage chainStage = (JoinStartChainStage)pipelineBuilder.addChainStage(new IStageBuilder<>() {

                @Override
                public AbstractChainStage<?> createStageChain(PipelineBuilder pipelineBuilder) {
                    JoinStartChainStage joinStartChainStage = new JoinStartChainStage();
                    joinStartChainStage.setLabel(NameCreatorContext.get().createName("join", "start"));
                    return joinStartChainStage;
                }

                @Override
                public void addConfigurables(PipelineBuilder pipelineBuilder) {

                }
            });
            String rightDependentTableName = rightResult.getBuilder().getTableName();
            chainStage.setRightDependentTableName(rightDependentTableName);
            if (right != null) {
                chainStage.setRightLabelName(right.getFirstStage().getLabel());
            } else {
                chainStage.setRightLabelName(NameCreatorContext.get().createName("right_join"));
            }

            pipelineBuilder.setHorizontalStages(chainStage);
            pipelineBuilder.setCurrentChainStage(chainStage);

            mergeSQLBuilderResult(right);
            pipelineBuilder.setRightJoin(true);
        } else {

            JoinWindow joinWindow = getOrCreateJoinWindow(piplineNamespace, piplineName, rightResult.getBuilder().getAsName());

            //这个分支对应的数据来源。key：数据来源的tablename，value：pipline。在解析阶段应用
            // Map<String, String> tableName2PipelineNames = new HashMap<>();
            //join的left流生成一个pipeline

            JoinStartChainStage startJoin = (JoinStartChainStage)pipelineBuilder.addChainStage(new IStageBuilder<>() {
                @Override
                public AbstractChainStage createStageChain(PipelineBuilder pipelineBuilder) {
                    JoinStartChainStage joinStartChainStage = new JoinStartChainStage();
                    joinStartChainStage.setLabel(NameCreatorContext.get().createName("join", "start"));
                    return joinStartChainStage;
                }

                @Override
                public void addConfigurables(PipelineBuilder pipelineBuilder) {

                }
            });

            SqlBuilderResult left = buildSQLBuilder(leftResult.getBuilder(), piplineNamespace, piplineName, joinWindow, true);
            if (left != null) {
                startJoin.setLeftLabelName(left.getFirstStage().getLabel());
            }

            SqlBuilderResult right = buildSQLBuilder(rightResult.getBuilder(), piplineNamespace, piplineName, joinWindow, false);
            if (right != null) {
                startJoin.setRightLabelName(right.getFirstStage().getLabel());
            }

            List<AbstractStage<?>> stages = new ArrayList<>();
            stages.add(startJoin);

            pipelineBuilder.setParentStageGroup(pipelineBuilder.getCurrentStageGroup());
            String rightDependentTableName;

            /**
             * 设置第一个节点
             */
            pipelineBuilder.setHorizontalStages(startJoin);
            pipelineBuilder.setCurrentChainStage(startJoin);
            /**
             * 把左流挂在第一个节点上
             */
            mergeSQLBuilderResult(left);

            //设置最后节点为join end

            pipelineBuilder.setCurrentChainStage(startJoin);
            mergeSQLBuilderResult(right);
            pipelineBuilder.setRightJoin(false);

            pipelineBuilder.setCurrentChainStage(left.getLastStage());

            boolean isSameTableName = leftResult.getBuilder().getTableName().equals(rightResult.getBuilder().getTableName());
            if (isSameTableName) {
                startJoin.setMsgSourceName(getMsgSourceName(leftResult.getBuilder()));
                rightDependentTableName = getMsgSourceName(rightResult.getBuilder());

            } else {
                rightDependentTableName = rightResult.getBuilder().getTableName();
            }
            startJoin.setRightDependentTableName(rightDependentTableName);
            joinWindow.setRightDependentTableName(startJoin.getRightDependentTableName());

        }

    }

    private JoinWindow getOrCreateJoinWindow(String piplineNamespace, String piplineName, String rightAsName) {
        if (this.joinWindow != null) {
            return this.joinWindow;
        }
        //创建共享的窗口，左右pipeline 通过输出到共享的窗口完成数据汇聚
        JoinWindow joinWindow = WindowBuilder.createDefaultJoinWindow();
        joinWindow.setNameSpace(piplineNamespace);
        String joinWindowName = NameCreatorContext.get().createName(piplineName, "join", "window");
        joinWindow.setName(joinWindowName);
        //是否有非等值的join 条件
        AtomicBoolean hasNoEqualsExpression = new AtomicBoolean(false);
        //把等值条件的左右字段映射成map
        Map<String, String> left2Right = createJoinFieldsFromCondition(onCondition, hasNoEqualsExpression);
        List<String> leftList = new ArrayList<>(left2Right.keySet());
        List<String> rightList = new ArrayList<>(left2Right.values());
        joinWindow.setLeftJoinFieldNames(leftList);
        joinWindow.setRightJoinFieldNames(rightList);

        joinWindow.setRightAsName(rightAsName);
        joinWindow.setJoinType(joinType);
        //如果有非等值，则把这个条件设置进去
        if (hasNoEqualsExpression.get()) {
            joinWindow.setExpression(onCondition);
        }
        pipelineBuilder.addConfigurables(joinWindow);
        this.joinWindow = joinWindow;
        return this.joinWindow;
    }

    protected String getMsgSourceName(AbstractSqlBuilder sqlBuilder) {
        String asName = sqlBuilder.getAsName();
        if (StringUtil.isEmpty(asName)) {
            asName = "";
        } else {
            asName = asName + ".";
        }
        return asName + sqlBuilder.getTableName();
    }

    protected SqlBuilderResult buildSQLBuilder(AbstractSqlBuilder sqlBuilder, String piplineNamespace, String piplineName, JoinWindow joinWindow, boolean isLeft) {
        String leftPipelineName = NameCreatorContext.get().createName(piplineName, "join", isLeft ? "left" : "rigth");
        PipelineBuilder pipelineBuilder = new PipelineBuilder(piplineNamespace, leftPipelineName, this.configuration);
        pipelineBuilder.setRightJoin(!isLeft);
        sqlBuilder.setPipelineBuilder(pipelineBuilder);
        pipelineBuilder.setRootTableName(this.pipelineBuilder.getRootTableName());
        pipelineBuilder.setParentTableName(this.pipelineBuilder.getParentTableName());
        sqlBuilder.setTableName2Builders(getTableName2Builders());

        SqlBuilderResult sqlBuilderResult = sqlBuilder.buildSql();
        if (sqlBuilderResult != null) {
            sqlBuilderResult.getConfigurables().remove(pipelineBuilder.getPipeline());
        }

        ShuffleChainStage joinWindowStage = (ShuffleChainStage)joinWindow.createStageChain(pipelineBuilder);
        pipelineBuilder.addChainStage(joinWindowStage.getOutputChainStage());

        // 这句一定要在addChainStage前，会默认赋值namespace和name
        joinWindow.addConfigurables(pipelineBuilder);
        joinWindowStage.setLabel(leftPipelineName);
        pipelineBuilder.setHorizontalStages(joinWindowStage.getOutputChainStage());
        if (isLeft) {
            pipelineBuilder.addChainStage(joinWindowStage.getConsumeChainStage());
            pipelineBuilder.setCurrentChainStage(joinWindowStage.getConsumeChainStage());
        }
        if (sqlBuilderResult != null) {
            sqlBuilderResult.setLastStage(joinWindowStage.getConsumeChainStage());
            sqlBuilderResult.setStages(pipelineBuilder.getPipeline().getStages());
            if (sqlBuilderResult.getFirstStage() == null || EmptyChainStage.class.isInstance(sqlBuilderResult.getFirstStage())) {
                sqlBuilderResult.setFirstStage(joinWindowStage.getOutputChainStage());
            }
        } else {
            return new SqlBuilderResult(pipelineBuilder, joinWindowStage.getOutputChainStage(), isLeft ? joinWindowStage.getConsumeChainStage() : null);
        }

        return sqlBuilderResult;
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
        BuilderParseResult leftResult = (BuilderParseResult)getLeft();
        BuilderParseResult rightResult = (BuilderParseResult)getRight();
        if (leftResult.getBuilder().getTableName().equals(parentName)) {
            return false;
        }
        if (rightResult.getBuilder().getTableName().equals(parentName)) {
            return true;
        }
        return false;
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
        Expression root = ExpressionBuilder.createOptimizationExpression("tmp", "tmp", onCondition, expressions, relationExpressions);
        List<Expression> expressionList = new ArrayList<>();
        if (RelationExpression.class.isInstance(root)) {
            RelationExpression relationExpression = (RelationExpression)root;
            if (relationExpression.getRelation().equals("or")) {
                throw new RuntimeException("join can not have or condition");
            }
            List<String> expressionNames = relationExpression.getValue();
            for (Expression expression : expressions) {
                if (expressionNames.contains(expression.getName())) {
                    expressionList.add(expression);
                }
            }

        } else {
            expressionList.add(root);
        }
        if (expressionList.size() == 0) {
            throw new RuntimeException("can not find join condition for shuffle join,join can not have 'or' condition");
        }
        if (expressionList.size() < expressions.size()) {
            hasNoEqualsExpression.set(true);
        }
        Map<String, String> left2Right = new HashMap<>();
        for (Expression<?> expression : expressionList) {
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
    protected String findByField(AbstractSqlBuilder builder, String varName) {
        if (builder instanceof SelectSqlBuilder) {
            SelectSqlBuilder selectSqlBuilder = (SelectSqlBuilder)builder;
            String value = selectSqlBuilder.findFieldBySelect(selectSqlBuilder, varName);
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
        AbstractSqlBuilder leftBuilder = getJoinBuilder(left);
        AbstractSqlBuilder rightBuilder = getJoinBuilder(right);

        if (leftBuilder == null || rightBuilder == null) {
            return false;
        }
        return !(rightBuilder instanceof SnapshotBuilder) && !(rightBuilder instanceof LateralTableBuilder);
    }

    /**
     * 判断是否是双流join，目前只支持自己join自己
     *
     * @return
     */
    private AbstractSqlBuilder getJoinBuilder(IParseResult<?> result) {
        if (result instanceof BuilderParseResult) {
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
        if (left instanceof BuilderParseResult) {
            BuilderParseResult result = (BuilderParseResult)left;
            if (result.getBuilder() instanceof SelectSqlBuilder) {
                SelectSqlBuilder selectSqlBuilder = (SelectSqlBuilder)result.getBuilder();
                if (aliasName == null || aliasName.equals(selectSqlBuilder.getAsName())) {
                    fields.addAll(selectSqlBuilder.getAllFieldNames());
                }

            }

        }
        if (right instanceof BuilderParseResult) {
            BuilderParseResult result = (BuilderParseResult)right;
            if (result.getBuilder() instanceof SelectSqlBuilder) {
                SelectSqlBuilder selectSqlBuilder = (SelectSqlBuilder)result.getBuilder();
                if (aliasName == null || aliasName.equals(selectSqlBuilder.getAsName())) {
                    String asName = selectSqlBuilder.getAsName();
                    if (asName == null) {
                        asName = "";
                    } else {
                        asName = asName + ".";
                    }
                    for (String fieldName : selectSqlBuilder.getAllFieldNames()) {
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
        if (left instanceof BuilderParseResult) {
            BuilderParseResult result = (BuilderParseResult)left;
            dependentTables.addAll(result.getBuilder().parseDependentTables());
        }
        if (right instanceof BuilderParseResult) {
            BuilderParseResult result = (BuilderParseResult)right;
            AbstractSqlBuilder rightBuilder = result.getBuilder();
            if (rightBuilder instanceof SnapshotBuilder || rightBuilder instanceof LateralTableBuilder) {
                return dependentTables;
            }
            dependentTables.addAll(result.getBuilder().parseDependentTables());
        }
        return dependentTables;
    }

    /**
     * 根据别名，返回全部字段
     *
     * @param aliasName
     * @param parseResult
     * @return
     */
    protected Set<String> findFieldsByAliasName(String aliasName, IParseResult<?> parseResult) {
        if (parseResult instanceof BuilderParseResult) {
            AbstractSqlBuilder builder = ((BuilderParseResult)getLeft()).getBuilder();
            if (!builder.getAsName().equals(aliasName)) {
                return null;
            }
            if (builder instanceof SelectSqlBuilder) {
                SelectSqlBuilder selectSqlBuilder = (SelectSqlBuilder)builder;
                return selectSqlBuilder.getAllFieldNames();
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
        String tableAsName = null;
        if (getLeft() instanceof BuilderParseResult) {
            BuilderParseResult builderParseResult = (BuilderParseResult)getLeft();
            tableAsName = builderParseResult.getBuilder().getAsName();
            if (asName != null && tableAsName == null) {
                tableAsName = builderParseResult.getBuilder().getTableName();
            }
            if ((asName != null && asName.equals(tableAsName)) | StringUtil.isEmpty(asName)) {
                if (builderParseResult.getBuilder() instanceof SelectSqlBuilder) {
                    SelectSqlBuilder selectSqlBuilder = (SelectSqlBuilder)builderParseResult.getBuilder();
                    if ("*".equals(fieldName)) {
                        return oriFieldName;
                    }
                    value = selectSqlBuilder.getFieldName(fieldName, true);
                }
                if (StringUtil.isNotEmpty(value)) {
                    return value;
                }
            }
        }
        if (getRight() instanceof BuilderParseResult) {
            BuilderParseResult builderParseResult = (BuilderParseResult)getRight();
            tableAsName = builderParseResult.getBuilder().getAsName();
            if (asName != null && tableAsName == null) {
                tableAsName = builderParseResult.getBuilder().getTableName();
            }
            if ((asName != null && asName.equals(tableAsName)) | StringUtil.isEmpty(asName)) {
                if (builderParseResult.getBuilder() instanceof SelectSqlBuilder) {
                    SelectSqlBuilder selectSqlBuilder = (SelectSqlBuilder)builderParseResult.getBuilder();
                    if ("*".equals(fieldName)) {
                        return oriFieldName;
                    }
                    value = selectSqlBuilder.getFieldName(fieldName, true);
                    if (StringUtil.isNotEmpty(value)) {
                        if (value.contains(".")) {
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

    @Override
    public String createSQLFromParser() {
        StringBuilder sb = new StringBuilder();
        sb.append(left.getResultValue() + " ");
        sb.append(joinType.equals("COMMA") ? "" : joinType + " ");
        if (BuilderParseResult.class.isInstance(right)) {
            BuilderParseResult builderParseResult = (BuilderParseResult)right;
            if (LateralTableBuilder.class.isInstance(builderParseResult.getBuilder())) {
                LateralTableBuilder lateralTableBuilder = (LateralTableBuilder)builderParseResult.getBuilder();
                sb.append("LATERAL TABLE(" + lateralTableBuilder.getLateralFunctionName() + "(...))");
            } else {
                sb.append(right.getReturnValue());
            }
        } else {
            sb.append(right.getReturnValue());
        }

        return sb.toString();
    }

    public IParseResult<?> getLeft() {
        return left;
    }

    public void setLeft(IParseResult<?> left) {
        this.left = left;
    }

    public IParseResult<?> getRight() {
        return right;
    }

    public void setRight(IParseResult<?> right) {
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

}
